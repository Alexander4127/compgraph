import typing as tp

from . import operations as ops
from . external_sort import ExternalSort


class Graph:
    """Computational graph implementation"""
    def __init__(self) -> None:
        self._op: tp.Optional[ops.Operation] = None
        self._prev: tp.Optional['Graph'] = None
        self._next: tp.Optional['Graph'] = None

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data from row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow
        Use ops.ReadIterFactory
        :param name: name of kwarg to use as data source
        """
        graph: 'Graph' = Graph()
        graph._op = ops.ReadIterFactory(name)
        return graph

    @staticmethod
    def graph_from_file(filename: str, parser: tp.Callable[[str], ops.TRow]) -> 'Graph':
        """Construct new graph extended with operation for reading rows from file
        Use ops.Read
        :param filename: filename to read from
        :param parser: parser from string to Row
        """
        graph: 'Graph' = Graph()
        graph._op = ops.Read(filename, parser)
        return graph

    def map(self, mapper: ops.Mapper) -> 'Graph':
        """Construct new graph extended with map operation with particular mapper
        :param mapper: mapper to use
        """
        graph: 'Graph' = Graph()
        graph._op = ops.Map(mapper)
        graph._prev = self
        return graph

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with reduce operation with particular reducer
        :param reducer: reducer to use
        :param keys: keys for grouping
        """
        graph: 'Graph' = Graph()
        graph._op = ops.Reduce(reducer, keys)
        graph._prev = self
        return graph

    def sort(self, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with sort operation
        :param keys: sorting keys (typical is tuple of strings)
        """
        graph: 'Graph' = Graph()
        graph._op = ExternalSort(keys)
        graph._prev = self
        return graph

    def join(self, joiner: ops.Joiner, join_graph: 'Graph', keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with join operation with another graph
        :param joiner: join strategy to use
        :param join_graph: other graph to join with
        :param keys: keys for grouping
        """
        graph: 'Graph' = Graph()
        graph._op = ops.Join(joiner, keys)
        graph._prev = self
        graph._next = join_graph
        return graph

    def run(self, **kwargs: tp.Any) -> ops.TRowsIterable:
        """Single method to start execution; data sources passed as kwargs"""
        if isinstance(self._op, ops.ReadIterFactory) or isinstance(self._op, ops.Read):
            assert self._prev is None and self._next is None, 'Static init should not have pre info'
            yield from self._op(**kwargs)
        elif isinstance(self._op, ops.Join):
            assert self._next is not None and self._prev is not None, 'Both vals to join are not nulls'
            yield from self._op(self._prev.run(**kwargs), self._next.run(**kwargs))
        else:
            assert isinstance(self._op, ops.Map) or \
                   isinstance(self._op, ops.Reduce) or \
                   isinstance(self._op, ExternalSort), 'Unknown operation'
            assert self._next is None and self._prev is not None, 'For map/reduce/sort need only first param'
            yield from self._op(self._prev.run(**kwargs))
