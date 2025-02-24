import random
from pathlib import Path

import typer
from rich import print
from rich.progress import track
from typing_extensions import Annotated

from falsa.local_fs import (
    NATIVE_I64_MAX_VALUE,
    GroupByGenerator,
    JoinBigGenerator,
    JoinMediumGenerator,
    JoinSmallGenerator,
    JoinLHSGenerator,
)
from falsa.utils import (
    DIVISORS,
    Format,
    Schemas,
    Size,
    clear_prev_if_exists,
    create_filename,
    generate_delta_log,
    get_writer,
)

help_str = """
[bold][green]H2O db-like-benchmark data generation.[/green][/bold]\n
[italic][red]This implementation is unofficial![/red][/italic]
For the official implementation please check https://github.com/duckdblabs/db-benchmark/tree/main/_data

Available commands are:
- [green]groupby[/green]: generate GroupBy dataset;
- [green]join[/green]: generate three Join datasets (small, medium, big);


Author: github.com/SemyonSinchenko
Source code: https://github.com/mrpowers-io/falsa
"""

app = typer.Typer(
    rich_markup_mode="rich",
    help=help_str,
)


@app.command(help="Create H2O GroupBy Dataset")
def groupby(
    path_prefix: Annotated[str, typer.Option(help="An output folder for generated data")],
    size: Annotated[Size, typer.Option(help="Dataset size: 0.5Gb, 5Gb, 50Gb")] = Size.SMALL,
    k: Annotated[int, typer.Option(help="An amount of keys (groups)")] = 100,
    nas: Annotated[int, typer.Option(min=0, max=100, help="A percentage of NULLS")] = 0,
    seed: Annotated[int, typer.Option(min=0, help="A seed of the generation")] = 42,
    batch_size: Annotated[int, typer.Option(min=0, help="A batch-size (in rows)")] = 5_000_000,
    data_format: Annotated[
        Format,
        typer.Option(help="An output format for generated data."),
    ] = Format.CSV,
):
    gb = GroupByGenerator(size._to(), k, nas, seed, batch_size)
    data_filename = create_filename("groupby", size._to().value, k, nas, data_format)
    output_dir = Path(path_prefix)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    output_filepath = output_dir.joinpath(data_filename)
    clear_prev_if_exists(output_filepath, data_format)

    print(f"{size._to().value} rows will be saved into: [green]{output_filepath.absolute().__str__()}[/green]\n")

    print("An output data [green]schema[/green] is the following:")
    print(Schemas.GROUPBY.value)
    print()

    writer = get_writer(data_format, Schemas.GROUPBY.value, output_filepath)
    for batch in track(gb.iter_batches(), total=len(gb.batches)):
        writer.write_batch(batch)
    writer.close()

    if data_format is Format.DELTA:
        generate_delta_log(output_filepath, Schemas.GROUPBY.value)


@app.command(help="Create three H2O join datasets")
def join(
    path_prefix: Annotated[str, typer.Option(help="An output folder for generated data")],
    size: Annotated[Size, typer.Option(help="Dataset size: 0.5Gb, 5Gb, 50Gb")] = Size.SMALL,
    k: Annotated[int, typer.Option(help="An amount of keys (groups)")] = 10,
    nas: Annotated[int, typer.Option(min=0, max=100, help="A percentage of NULLS")] = 0,
    seed: Annotated[int, typer.Option(min=0, help="A seed of the generation")] = 42,
    batch_size: Annotated[int, typer.Option(min=0, help="A batch-size (in rows)")] = 5_000_000,
    data_format: Annotated[
        Format,
        typer.Option(help="An output format for generated data."),
    ] = Format.CSV,
):
    random.seed(seed)
    keys_seed = random.randint(0, NATIVE_I64_MAX_VALUE)
    generation_seed = random.randint(0, NATIVE_I64_MAX_VALUE)
    n_small = size._to() // DIVISORS["join_small"]
    n_medium = size._to() // DIVISORS["join_medium"]
    n_big = size._to() // DIVISORS["join_big"]
    join_lhs = JoinLHSGenerator(size._to(), n_small, k, generation_seed, keys_seed, min([batch_size, n_small]))
    join_small = JoinSmallGenerator(size._to(), n_small, k, generation_seed, keys_seed, min([batch_size, n_small]))
    join_medium = JoinMediumGenerator(size._to(), n_medium, k, generation_seed, keys_seed, min([batch_size, n_medium]))
    join_big = JoinBigGenerator(size._to(), n_big, k, nas, generation_seed, keys_seed, min([batch_size, n_big]))

    data_filename_small = create_filename("join_small", size._to().value, k, nas, data_format)
    data_filename_medium = create_filename("join_medium", size._to().value, k, nas, data_format)
    data_filename_big = create_filename("join_big", size._to().value, k, nas, data_format)
    data_filename_lhs = create_filename("join_lhs", size._to().value, k, nas, data_format)

    output_dir = Path(path_prefix)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    output_small = output_dir.joinpath(data_filename_small)
    output_medium = output_dir.joinpath(data_filename_medium)
    output_big = output_dir.joinpath(data_filename_big)
    output_lhs = output_dir.joinpath(data_filename_lhs)

    clear_prev_if_exists(output_lhs, data_format)
    clear_prev_if_exists(output_small, data_format)
    clear_prev_if_exists(output_medium, data_format)
    clear_prev_if_exists(output_big, data_format)

    print(
        f"{size._to().value // DIVISORS['join_small']} rows will be saved into: [green]{output_small.absolute().__str__()}[/green]\n"
    )
    print(
        f"{size._to().value // DIVISORS['join_medium']} rows will be saved into: [green]{output_medium.absolute().__str__()}[/green]\n"
    )
    print(
        f"{size._to().value // DIVISORS['join_big']} rows will be saved into: [green]{output_big.absolute().__str__()}[/green]\n"
    )

    print("An [bold]SMALL[/bold] data [green]schema[/green] is the following:")
    print(Schemas.JOIN_RHS_SMALL.value)
    print()
    writer_small = get_writer(
        output_filepath=output_small, data_format=data_format, schema=Schemas.JOIN_RHS_SMALL.value
    )

    for batch in track(join_small.iter_batches(), total=len(join_small.batches)):
        writer_small.write_batch(batch)
    writer_small.close()

    if data_format is Format.DELTA:
        generate_delta_log(output_small, Schemas.JOIN_RHS_SMALL.value)

    print()
    print("An [bold]MEDIUM[/bold] data [green]schema[/green] is the following:")
    print(Schemas.JOIN_RHS_MEDIUM.value)
    print()

    writer_medium = get_writer(
        output_filepath=output_medium, data_format=data_format, schema=Schemas.JOIN_RHS_MEDIUM.value
    )

    for batch in track(join_medium.iter_batches(), total=len(join_medium.batches)):
        writer_medium.write_batch(batch)
    writer_medium.close()

    if data_format is Format.DELTA:
        generate_delta_log(output_medium, Schemas.JOIN_RHS_MEDIUM.value)

    print()
    print("An [bold]BIG[/bold] data [green]schema[/green] is the following:")
    print(Schemas.JOIN_RHS_BIG.value)
    print()

    writer_big = get_writer(output_filepath=output_big, data_format=data_format, schema=Schemas.JOIN_RHS_BIG.value)

    for batch in track(join_big.iter_batches(), total=len(join_big.batches)):
        writer_big.write_batch(batch)
    writer_big.close()

    if data_format is Format.DELTA:
        generate_delta_log(output_big, Schemas.JOIN_RHS_BIG.value)

    print()
    print("An [bold]LSH[/bold] data [green]schema[/green] is the following:")
    print(Schemas.JOIN_LHS.value)
    print()

    writer_lsh = get_writer(output_filepath=output_lhs, data_format=data_format, schema=Schemas.JOIN_LHS.value)

    for batch in track(join_lhs.iter_batches(), total=len(join_lhs.batches)):
        writer_lsh.write_batch(batch)
    writer_lsh.close()

    if data_format is Format.DELTA:
        generate_delta_log(output_lhs, Schemas.JOIN_LHS.value)


def entry_point() -> None:
    app()


if __name__ == "__main__":
    entry_point()
