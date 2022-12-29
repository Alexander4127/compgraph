# Computation Graph Library

Package includes computational graph implementation with examples.
Library supports `Map` / `Reduce` semantics and requires 
constant memory to process any size of input data.

## Getting Started

To get started clone repo and print command
```bash
pip install -e compgraph --force-reinstall
```

Example of using graph (graph methods defined in `compgraph/graph.py`) for 
calculating number of words in documents
```python
graph: Graph = Graph.graph_from_iter('docs') \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])

documents: list[TRow] = [
    {'doc_id': 1, 'text': 'text 1'},
    {'doc_id': 2, 'text': 'text 2'}
]

assert sorted(list(graph.run(docs=lambda: iter(documents)))) == [
    {'count': 1, 'text': '1'}
    {'count': 1, 'text': '2'},
    {'count': 2, 'text': 'text'},
]
```

__Note1__: It should be stated that generators supposed to be renewable, because 
graph structure in case of sorting and joining is nonlinear.

__Note2__: Before applying `join` and `reduce` methods, all involved 
tables have to be sorted by appropriate keys.

### Prerequisites

For a correct compilation of typings, Python 3.10 and above versions are recommended.

Library requires installed `click` package to provide correct 
work of examples. All requirements can be installed using
```bash
pip install -r requirements.txt
```

## Examples

As mentioned above, some graphs are already implemented in `compgraph/algorithms.py`.
Besides, in `examples` are located python scripts, which allows to use graphs by typing
```bash
python3 run_word_count input_filename output_filename
```
For `run_maps.py` there are two input files.

### Style

All functions and classes have doc strings and typing to 
simpler understanding. To check codestyle was applied
```bash
mypy compgraph
flake8 compgraph
```
