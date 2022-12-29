import click
from json import loads, dumps

from compgraph.algorithms import word_count_graph
from compgraph.operations import TRow, Read


def to_trow(row: str) -> TRow:
    return loads(row)


@click.command()
@click.argument('input_filepath')
@click.argument('output_filepath')
def cli(input_filepath: str, output_filepath: str) -> None:
    graph = word_count_graph(input_stream_name='input')

    with open(output_filepath, 'w') as out:
        kwargs = {'input': Read(input_filepath, to_trow)}
        for row in graph.run(**kwargs):
            print(dumps(row), file=out)


if __name__ == '__main__':
    cli()
