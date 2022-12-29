import typing as tp

from .base import Operation, TRow, TRowsGenerator


__all__ = [
    'Read',
    'ReadIterFactory'
]


class Read(Operation):
    """Generator of parsed rows from file"""

    def __init__(self, filename: str, parser: tp.Callable[[str], TRow]) -> None:
        """
        :param filename: File to read from
        :param parser: Parser used to make TRow from string
        """
        self.filename = filename
        self.parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        with open(self.filename) as f:
            for line in f:
                yield self.parser(line)


class ReadIterFactory(Operation):
    """Generator of rows from key-word argument"""

    def __init__(self, name: str) -> None:
        """
        :param name: Key of argument
        """
        self.name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        for row in kwargs[self.name]():
            yield row
