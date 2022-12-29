from math import log

from . import Graph, operations


def word_count_graph(input_stream_name: str, text_column: str = 'text', count_column: str = 'count') -> Graph:
    """Constructs graph which counts words in text_column of all rows passed"""
    return Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def inverted_index_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
                         result_column: str = 'tf_idf') -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair"""
    # Splitting words
    split_words: Graph = Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column))

    # Calculating number of documents
    doc_count: Graph = Graph.graph_from_iter(input_stream_name) \
        .sort((doc_column,)) \
        .reduce(operations.FirstReducer(), (doc_column,)) \
        .reduce(operations.Count('num_docs'), tuple())

    # Calculating idf index by number of words per one doc and doc number
    count_idf = split_words.sort([text_column, doc_column]) \
        .reduce(operations.FirstReducer(), (text_column, doc_column)) \
        .reduce(operations.Count('num_words_for_doc'), (text_column,)) \
        .join(operations.InnerJoiner(), doc_count, tuple()) \
        .map(operations.Apply(
            ('num_docs', 'num_words_for_doc'),
            'idf',
            lambda num_docs, num_words_for_doc: log(num_docs / num_words_for_doc)
        )
    )

    # Calculating tf-idf index
    # 1. Collecting tf indexes
    # 2. Merging them with idf indexes by text columns
    # 3. Applying lambda and selecting top results
    tf_idf: Graph = split_words.sort((doc_column,)) \
        .reduce(operations.TermFrequency(text_column, 'tf'), (doc_column,)) \
        .sort((text_column,)) \
        .join(operations.InnerJoiner(), count_idf, (text_column,)) \
        .map(operations.Apply(('tf', 'idf'), result_column, lambda tf, idf: tf * idf)) \
        .map(operations.Project((text_column, doc_column, result_column))) \
        .reduce(operations.TopN(result_column, 3), (text_column,))

    return tf_idf


def pmi_graph(input_stream_name: str, doc_column: str = 'doc_id', text_column: str = 'text',
              result_column: str = 'pmi') -> Graph:
    """Constructs graph which gives for every document the top 10 words ranked by pointwise mutual information"""
    # Splitting words and filtering the shortest
    split_words: Graph = Graph.graph_from_iter(input_stream_name) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .map(operations.Filter(lambda row: len(row[text_column]) > 4))

    # Making indexation for further correct ordering
    indexed_words: Graph = split_words.reduce(operations.Index('index'), tuple())

    # Filtering words by occurrences in docs
    filtered_count: Graph = indexed_words.sort([text_column, doc_column]) \
        .reduce(operations.Count('num_words_for_doc'), (text_column, doc_column)) \
        .map(operations.Filter(lambda row: row['num_words_for_doc'] >= 2))

    # Merging words with filtered version to clear data
    filtered_table: Graph = indexed_words.sort((text_column, doc_column)).join(
        operations.InnerJoiner(),
        filtered_count,
        (text_column, doc_column)
    )

    # Filtering columns
    filtered_table = filtered_table.map(operations.Project((text_column, doc_column)))

    # Frequency per doc (equals tf index)
    tf: Graph = filtered_table.sort((doc_column,)).reduce(operations.TermFrequency(text_column, 'tf'), (doc_column,))

    # Number of word per doc (previous was incorrect)
    num_words_for_doc: Graph = filtered_table.sort((text_column, doc_column)) \
        .reduce(operations.Count('num_words_for_doc'), (text_column,))

    # Calculating pmi and deleting others
    added_all_number: Graph = num_words_for_doc.join(
        operations.InnerJoiner(),
        filtered_table.reduce(operations.Count('all_numb_words'), tuple()),
        tuple()
    )

    # Calculating pmi and deleting others
    added_pmi_metric: Graph = added_all_number.join(operations.InnerJoiner(), tf.sort((text_column,)), (text_column,)) \
        .map(operations.Apply(
            ('num_words_for_doc', 'all_numb_words', 'tf'),
            result_column,
            lambda nwfd, anw, tf_ind: log(tf_ind / (nwfd / anw))
        )) \
        .map(operations.Project((text_column, doc_column, result_column))) \
        .sort((doc_column, text_column))

    # Preparing indexes
    sorted_words: Graph = indexed_words.sort((doc_column, text_column)) \
        .reduce(operations.FirstReducer(), (doc_column, text_column))

    # Recovering initial order
    pmi: Graph = added_pmi_metric.join(
        operations.InnerJoiner(),
        sorted_words,
        (doc_column, text_column)
    ).sort(('index',)).map(operations.Project((doc_column, text_column, result_column)))

    return pmi


def yandex_maps_graph(input_stream_name_time: str, input_stream_name_length: str,
                      enter_time_column: str = 'enter_time', leave_time_column: str = 'leave_time',
                      edge_id_column: str = 'edge_id', start_coord_column: str = 'start', end_coord_column: str = 'end',
                      weekday_result_column: str = 'weekday', hour_result_column: str = 'hour',
                      speed_result_column: str = 'speed') -> Graph:
    """Constructs graph which measures average speed in km/h depending on the weekday and hour"""

    # Calculating lengths using haversine distance
    length: Graph = Graph.graph_from_iter(input_stream_name_length) \
        .map(operations.HaversineDist(start_coord_column, end_coord_column, 'length')) \
        .map(operations.Project((edge_id_column, 'length'))) \
        .sort((edge_id_column,))

    # Calculating duration
    duration: Graph = Graph.graph_from_iter(input_stream_name_time) \
        .map(operations.StringToDateTime([enter_time_column, leave_time_column])) \
        .map(operations.Apply(
            (enter_time_column, leave_time_column),
            'duration',
            lambda t1, t2: (t2 - t1).total_seconds() / 3600)
        ) \
        .map(operations.Apply((enter_time_column,), hour_result_column, lambda t: t.hour)) \
        .map(operations.Apply((enter_time_column,), weekday_result_column, lambda t: t.strftime('%a'))) \
        .map(operations.Remove((enter_time_column, leave_time_column))) \
        .sort((edge_id_column,))

    # Merging results by edge_id, calculating speed, selecting columns
    merged: Graph = duration.join(operations.InnerJoiner(), length, (edge_id_column,)) \
        .sort((weekday_result_column, hour_result_column)) \
        .reduce(operations.MeanSpeed('length', 'duration', speed_result_column),
                (weekday_result_column, hour_result_column)) \
        .map(operations.Remove(('length', 'duration')))

    return merged
