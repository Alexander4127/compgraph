import typing as tp
import heapq
from collections import defaultdict

from .base import TRow, TRowsIterable, TRowsGenerator, Reducer


__all__ = [
    'FirstReducer',
    'TopN',
    'TermFrequency',
    'Count',
    'Index',
    'Sum',
    'Mean',
    'MeanSpeed'
]


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        for row in rows:
            yield row
            break


class TopN(Reducer):
    """Calculate top N by value"""

    def __init__(self, column: str, n: int) -> None:
        """
        :param column: column name to get top by
        :param n: number of top values to extract
        """
        self.column_max = column
        self.n = n

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        sorted_lst: list[tuple[tp.Any, ...]] = []
        i: int = 0
        for row in rows:
            el: tuple[tp.Any, ...] = (row[self.column_max], -i, row)
            if len(sorted_lst) < self.n:
                heapq.heappush(sorted_lst, el)
            elif row[self.column_max] > sorted_lst[0][0]:
                heapq.heappop(sorted_lst)
                heapq.heappush(sorted_lst, el)
            i += 1
        for a, b, row in sorted_lst:
            yield row


class TermFrequency(Reducer):
    """Calculate frequency of values in column"""

    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """
        :param words_column: name for column with words
        :param result_column: name for result column
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        counts: defaultdict[tp.Any, int] = defaultdict(int)
        counter: int = 0
        return_row: TRow = {}
        for row in rows:
            if not return_row:
                return_row = {k: row[k] for k in group_key}
            counts[row[self.words_column]] += 1
            counter += 1
        for k, v in counts.items():
            cur_row = return_row.copy()
            cur_row[self.words_column] = k
            cur_row[self.result_column] = v / counter
            yield cur_row


class Count(Reducer):
    """
    Count records by key
    Example for group_key=('a',) and column='d'
        {'a': 1, 'b': 5, 'c': 2}
        {'a': 1, 'b': 6, 'c': 1}
        =>
        {'a': 1, 'd': 2}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for result column
        """
        self.column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        result_row: TRow = {}
        for row in rows:
            if not result_row:
                result_row = {k: row[k] for k in group_key if k in row}
                result_row[self.column] = 1
            else:
                result_row[self.column] += 1
        yield result_row


class Index(Reducer):
    """Make indexation column in table"""

    def __init__(self, column: str) -> None:
        """
        :param column: name for result column
        """
        self.column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        index: int = 0
        for row in rows:
            result_row: TRow = row.copy()
            result_row[self.column] = index
            index += 1
            yield result_row


class Sum(Reducer):
    """
    Sum values aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 5}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for sum column
        """
        self.column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        result_row: TRow = {}
        for row in rows:
            if not result_row:
                result_row = {k: row[k] for k in group_key}
                result_row[self.column] = row[self.column]
            else:
                result_row[self.column] += row[self.column]
        yield result_row


class Mean(Reducer):
    """
    Mean value aggregated by key
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 2.5}
    """

    def __init__(self, column: str) -> None:
        """
        :param column: name for mean column
        """
        self.column = column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        result_row: TRow = {}
        n: int = 0
        for row in rows:
            n += 1
            if not result_row:
                result_row = {k: row[k] for k in group_key}
                result_row[self.column] = row[self.column]
            else:
                result_row[self.column] += row[self.column]
        result_row[self.column] /= n
        yield result_row


class MeanSpeed(Reducer):
    """
    Mean speed aggregated by key
    Example for key=('a',) and dist_column='d', time_column='t', result_column='ms'
        {'a': 1, 'd': 10, 't': 6}
        {'a': 1, 'd': 15, 't': 4}
        =>
        {'a': 1, 'b': 2.5}
    """

    def __init__(self, dist_column: str, time_column: str, result_column: str) -> None:
        """
        :param dist_column: name for column with distances
        :param time_column: name for column with times
        :param result_column: column to write result
        """
        self.dist_column = dist_column
        self.time_column = time_column
        self.result_column = result_column

    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        result_row: TRow = {}
        sum_dists: float = 0.
        sum_times: float = 0.
        for row in rows:
            if not result_row:
                result_row = {k: row[k] for k in group_key}
            sum_dists += row[self.dist_column]
            sum_times += row[self.time_column]
        result_row[self.result_column] = sum_dists / sum_times
        yield result_row
