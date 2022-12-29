import typing as tp
import re
import string
import datetime
from math import radians, cos, sin, asin, sqrt

from .base import TRowsGenerator, TRow, Mapper


__all__ = [
    "DummyMapper",
    "FilterPunctuation",
    "LowerCase",
    "Split",
    "Product",
    "Filter",
    "Project",
    "Apply",
    'StringToDateTime',
    'HaversineDist',
    'Remove'
]


class DummyMapper(Mapper):
    """Yield exactly the row passed"""

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield row


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        regex = re.compile('[%s]' % re.escape(string.punctuation))
        row[self.column] = regex.sub('', row[self.column])
        yield row


class LowerCase(Mapper):
    """Replace column value with value in lower case"""

    def __init__(self, column: str):
        """
        :param column: name of column to process
        """
        self.column = column

    @staticmethod
    def _lower_case(txt: str) -> str:
        return txt.lower()

    def __call__(self, row: TRow) -> TRowsGenerator:
        row[self.column] = self._lower_case(row[self.column])
        yield row


class Split(Mapper):
    """Split row on multiple rows by separator"""

    def __init__(self, column: str, separator: str | None = None) -> None:
        """
        :param column: name of column to split
        :param separator: string to separate by
        """
        self.column = column
        self.separator = re.compile(separator) if separator is not None else re.compile(r'[\s+]')

    def __call__(self, row: TRow) -> TRowsGenerator:
        text: str = row[self.column]
        prev: int = 0
        row = row.copy()
        for cur in re.finditer(self.separator, text):
            row[self.column] = text[prev:cur.start()]
            yield row
            prev = cur.end()
            row = row.copy()
        if len(text) != prev:
            row[self.column] = text[prev:]
            yield row


class Product(Mapper):
    """Calculates product of multiple columns"""

    def __init__(self, columns: tp.Sequence[str], result_column: str = 'product') -> None:
        """
        :param columns: column names to product
        :param result_column: column name to save product in
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row = row.copy()
        prod_val = 1
        for col in self.columns:
            prod_val *= row[col]
        row[self.result_column] = prod_val
        yield row


class Filter(Mapper):
    """Remove records that don't satisfy some condition"""

    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """
        :param condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        if self.condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns"""

    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {k: row[k] for k in self.columns}


class Apply(Mapper):
    """Apply given function"""

    def __init__(self, columns: tp.Sequence[str], result_column: str, func: tp.Callable[..., tp.Any]):
        """
        :param columns: names of columns with arguments
        :param result_column: name of column to write result
        :param func: function applied to columns
        """
        self.columns = columns
        self.result_column = result_column
        self.func = func

    def __call__(self, row: TRow) -> TRowsGenerator:
        args = [row[column] for column in self.columns]
        result_row = row.copy()
        result_row[self.result_column] = self.func(*args)
        yield result_row


class StringToDateTime(Mapper):
    """Convert UTC time string to datetime"""

    def __init__(self, columns: list[str]) -> None:
        """
        :param columns: columns with strings to convert
        """
        self.columns = columns

    @staticmethod
    def _make_datetime(s: str) -> datetime.datetime:
        try:
            return datetime.datetime.strptime(s, '%Y%m%dT%H%M%S.%f')
        except ValueError:
            return datetime.datetime.strptime(s, '%Y%m%dT%H%M%S')

    def __call__(self, row: TRow) -> TRowsGenerator:
        row = row.copy()
        for column in self.columns:
            row[column] = self._make_datetime(row[column])
        yield row


class HaversineDist(Mapper):
    """Calculate haversine dist between two points"""
    EARTH_RADIUS_KM = 6373

    def __init__(self, start_column: str, end_column: str, column: str):
        """
        :param start_column: start point with coord (longitude, latitude)
        :param end_column: end point with coord (longitude, latitude)
        :param column:
        """
        self.start = start_column
        self.end = end_column
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        row = row.copy()
        row[self.column] = self._haversine_dist(*row[self.start], *row[self.end])
        yield row

    @classmethod
    def _haversine_dist(cls, lng1: float, lat1: float, lng2: float, lat2: float) -> float:
        """
        Calculate haversine distance between (lat1, lng1) and (lat2, lng2)
        """

        lat1, lng1, lat2, lng2 = map(radians, (lat1, lng1, lat2, lng2))
        lat = lat2 - lat1
        lng = lng2 - lng1

        d = sin(lat * 0.5) ** 2 + cos(lat1) * cos(lat2) * sin(lng * 0.5) ** 2
        return 2 * cls.EARTH_RADIUS_KM * asin(sqrt(d))


class Remove(Mapper):
    """Leave only not mentioned columns"""

    def __init__(self, columns: tp.Sequence[str]) -> None:
        """
        :param columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        yield {k: row[k] for k in row if k not in self.columns}
