import click
from json import loads, dumps

from compgraph.algorithms import yandex_maps_graph
from compgraph.operations import TRow, Read


def to_trow(row: str) -> TRow:
    return loads(row)


@click.command()
@click.argument('input_filepath_len')
@click.argument('input_filepath_time')
@click.argument('output_filepath')
def cli(input_filepath_len: str, input_filepath_time: str, output_filepath: str) -> None:
    graph = yandex_maps_graph(input_stream_name_time='time', input_stream_name_length='length')

    with open(output_filepath, 'w') as out:
        kwargs = {'length': Read(input_filepath_len, to_trow), 'time': Read(input_filepath_time, to_trow)}
        for row in graph.run(**kwargs):
            print(dumps(row), file=out)


if __name__ == '__main__':
    cli()
