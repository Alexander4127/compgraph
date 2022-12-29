import typing as tp

from .base import TRowsIterable, TRowsGenerator, Joiner


__all__ = [
    'InnerJoiner',
    'OuterJoiner',
    'RightJoiner',
    'LeftJoiner'
]


class InnerJoiner(Joiner):
    """Join with inner strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        yield from self.prod_tables(keys, rows_a, list(rows_b))


class OuterJoiner(Joiner):
    """Join with outer strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_a, rows_b = list(rows_a), list(rows_b)

        yield from self.prod_tables(keys, rows_a, rows_b)

        if not rows_a:
            yield from rows_b

        if not rows_b:
            yield from rows_a


class LeftJoiner(Joiner):
    """Join with left strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_b = list(rows_b)

        yield from self.prod_tables(keys, rows_a, rows_b)

        if not rows_b:
            yield from rows_a


class RightJoiner(Joiner):
    """Join with right strategy"""

    def __call__(self, keys: tp.Sequence[str], rows_a: TRowsIterable, rows_b: TRowsIterable) -> TRowsGenerator:
        rows_b = list(rows_b)

        yield from self.prod_tables(keys, rows_a, rows_b)

        if not rows_a:
            yield from rows_b
