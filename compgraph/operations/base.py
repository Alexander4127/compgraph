import typing as tp
from itertools import groupby
from operator import itemgetter
from abc import abstractmethod, ABC


TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


__all__ = [
    'Operation',
    'Mapper',
    'Map',
    'Reducer',
    'Reduce',
    'Joiner',
    'Join',
    'TRowsGenerator',
    'TRowsIterable',
    'TRow'
]


def safe_itemgetter(keys: tp.Sequence[str]) -> tp.Any:
    """
    Return getter, safe for empty sequence
    :param keys: tuple of keys
    """
    if not keys:
        return lambda _: []
    return itemgetter(*keys)


class Operation(ABC):
    """Base class for all operations"""

    @abstractmethod
    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        pass


class Mapper(ABC):
    """Base class for mappers"""

    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """
        :param row: one table row
        """
        pass


class Map(Operation):
    def __init__(self, mapper: Mapper) -> None:
        """
        :param mapper: mapper to use
        """
        self.mapper = mapper

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in rows:
            yield from self.mapper(row)


class Reducer(ABC):
    """Base class for reducers"""

    @abstractmethod
    def __call__(self, group_key: tuple[str, ...], rows: TRowsIterable) -> TRowsGenerator:
        """
        :param rows: table rows
        """
        pass


class Reduce(Operation):
    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        """
        :param reducer: reducer to apply to equal keys sets
        :param keys: set of keys to group by
        """
        self.reducer = reducer
        self.keys = keys

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """
        :param keys: keys to group by
        :param rows: table rows
        """
        for key, group_items in groupby(rows, key=safe_itemgetter(self.keys)):
            yield from self.reducer(tuple(self.keys), group_items)


class Joiner(ABC):
    """Base class for joiners"""

    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        """
        Use suffixes to equal column names which are not included in keys
        :param suffix_a: suffix to add in left table
        :param suffix_b: suffix to add in right table
        """
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @abstractmethod
    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        """
        :param keys: join keys
        :param rows_a: left table rows
        :param rows_b: right table rows
        """
        pass

    def prod_tables(self, keys: tp.Sequence[str], rows_a: tp.Iterable[TRow], rows_b: tp.List[TRow]) -> TRowsGenerator:
        """
        Calculate cartesian products of tables with equal keys
        :param keys: keys to group by
        :param rows_a: generator of rows from first table
        :param rows_b: list of rows from second table
        """
        if not rows_b:
            return
        for a in rows_a:
            for b in rows_b:
                row: dict[str, tp.Any] = {k: a[k] for k in keys}
                inter: set[str] = set(a.keys()) & set(b.keys())
                common: set[str] = inter - set(keys)
                for k in common:
                    row[k + self._a_suffix] = a[k]
                    row[k + self._b_suffix] = b[k]
                for k in set(a.keys()) - inter:
                    row[k] = a[k]
                for k in set(b.keys()) - inter:
                    row[k] = b[k]
                yield row


class Join(Operation):
    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        """
        :pram joiner: joiner for merging tables
        :param keys: set of keys to group by
        """
        self.joiner = joiner
        self.keys = keys

    def _comparator(self, row: TRow) -> list[tp.Any]:
        return [row[key] for key in self.keys]

    @staticmethod
    def _next_iter(iterator: tp.Any) -> tuple[list[tp.Any] | None, TRowsIterable | None]:
        try:
            return next(iterator)
        except StopIteration:
            return None, None

    def __call__(self, rows: TRowsIterable, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """
        :param rows: left table to join
        :param args[0]: right table to join
        """
        rows_a = iter(groupby(rows, key=self._comparator))
        rows_b = iter(groupby(args[0], key=self._comparator))

        ak, ag = self._next_iter(rows_a)
        bk, bg = self._next_iter(rows_b)

        while ag is not None and bg is not None:
            assert ak is not None and bk is not None

            if ak < bk:
                yield from self.joiner(self.keys, ag, [])
                ak, ag = self._next_iter(rows_a)
                continue

            if bk < ak:
                yield from self.joiner(self.keys, [], bg)
                bk, bg = self._next_iter(rows_b)
                continue

            yield from self.joiner(self.keys, ag, bg)
            ak, ag = self._next_iter(rows_a)
            bk, bg = self._next_iter(rows_b)

        while ag is not None:
            yield from self.joiner(self.keys, ag, [])
            ak, ag = self._next_iter(rows_a)

        while bg is not None:
            yield from self.joiner(self.keys, [], bg)
            bk, bg = self._next_iter(rows_b)
