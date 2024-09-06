import random
import shutil
from enum import Enum
from pathlib import Path

import pyarrow as pa
import typer
from pyarrow import csv, parquet
from rich import print
from rich.progress import track
from typing_extensions import Annotated

from falsa import H2ODatasetSizes
from falsa.local_fs import (
    NATIVE_I64_MAX_VALUE,
    GroupByGenerator,
    JoinBigGenerator,
    JoinMediumGenerator,
    JoinSmallGenerator,
)

from .utils import generate_delta_log

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


class Size(str, Enum):
    SMALL = "SMALL"
    MEDIUM = "MEDIUM"
    BIG = "BIG"

    def _to(self) -> H2ODatasetSizes:
        # Workaround. Typer does not support IntEnum
        if self is Size.SMALL:
            return H2ODatasetSizes.SMALL
        elif self is Size.MEDIUM:
            return H2ODatasetSizes.MEDIUM
        else:
            return H2ODatasetSizes.BIG


class Format(str, Enum):
    CSV = "CSV"
    DELTA = "DELTA"
    PARQUET = "PARQUET"

    def pprint(self):
        print(f"An output format is [green]{self.value}[/green]")
        print("\nBatch mode is supported.")
        print("In case of memory problems you can try to reduce a [green]batch_size[/green].")
        print()


# An amount of rows per dataset is n // divisor
_DIVISORS = {
    "groupby": 1,
    "join_big": 1,
    "join_big_na": 1,
    "join_small": 1_000_000,
    "join_medium": 1_000,
}


def _pretty_sci(n: int) -> str:
    # See https://github.com/duckdblabs/db-benchmark/blob/main/_data/groupby-datagen.R#L5
    # pretty_sci = function(x) {
    #     tmp<-strsplit(as.character(x), "+", fixed=TRUE)[[1L]]
    #     if(length(tmp)==1L) {
    #             paste0(substr(tmp, 1L, 1L), "e", nchar(tmp)-1L)
    #     } else if(length(tmp)==2L){
    #         paste0(tmp[1L], as.character(as.integer(tmp[2L])))
    #     }
    # }
    if n == 0:
        return "NA"

    # format in scientific notation and remove +
    formatted_num = f"{n:.0e}".replace("+", "")
    e_value = int(formatted_num.split("e")[1])

    if e_value >= 10:
        return formatted_num

    elif e_value == 0:
        return formatted_num.replace("00", "0")

    elif e_value < 10:
        return formatted_num.replace("0", "")

    else:
        raise ValueError("Unexpected value following e")


def _create_filename(ds_type: str, n: int, k: int, nas: int, fmt: Format) -> str:
    if fmt is Format.DELTA:
        suffix = ""
    else:
        suffix = "." + fmt.lower()
    output_names = {
        "groupby": "G1_{n}_{n}_{k}_{nas}{fmt}",
        "join_big": "J1_{n}_{n}_NA{fmt}",
        "join_big_na": "J1_{n}_{n}_{nas}{fmt}",
        "join_small": "J1_{n}_{n_divided}_{nas}{fmt}",
        "join_medium": "J1_{n}_{n_divided}_{nas}{fmt}",
    }

    n_divisor = _DIVISORS[ds_type]
    n_divided = n // n_divisor
    return output_names[ds_type].format(n=_pretty_sci(n), n_divided=_pretty_sci(n_divided), k=k, nas=nas, fmt=suffix)


def _clear_prev_if_exists(fp: Path, fmt: Format) -> None:
    if fp.exists():
        # All is file, delta is directory
        if fmt is not Format.DELTA:
            fp.unlink()
        else:
            shutil.rmtree(fp, ignore_errors=True)


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
    data_filename = _create_filename("groupby", size._to().value, k, nas, data_format)
    output_dir = Path(path_prefix)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    output_filepath = output_dir.joinpath(data_filename)
    _clear_prev_if_exists(output_filepath, data_format)

    print(f"{size._to().value} rows will be saved into: [green]{output_filepath.absolute().__str__()}[/green]\n")

    schema = pa.schema(
        [
            ("id1", pa.utf8()),
            ("id2", pa.utf8()),
            ("id3", pa.utf8()),
            ("id4", pa.int64()),
            ("id5", pa.int64()),
            ("id6", pa.int64()),
            ("v1", pa.int64(), False),
            ("v2", pa.int64(), False),
            ("v3", pa.float64(), False),
        ]
    )

    print("An output data [green]schema[/green] is the following:")
    print(schema)
    print()

    data_format.pprint()
    print()
    if data_format is Format.CSV:
        writer = csv.CSVWriter(sink=output_filepath, schema=schema)
        for batch in track(gb.iter_batches(), total=len(gb.batches)):
            writer.write_batch(batch)

        writer.close()

    if data_format is Format.PARQUET:
        writer = parquet.ParquetWriter(where=output_filepath, schema=schema)
        for batch in track(gb.iter_batches(), total=len(gb.batches)):
            writer.write_batch(batch)

        writer.close()

    if data_format is Format.DELTA:
        output_filepath.mkdir(parents=True)
        delta_file_pq = output_filepath.joinpath("data.parquet")
        writer = parquet.ParquetWriter(where=delta_file_pq, schema=schema)
        for batch in track(gb.iter_batches(), total=len(gb.batches)):
            writer.write_batch(batch)

        writer.close()
        generate_delta_log(output_filepath, schema)


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
    n_small = size._to() // _DIVISORS["join_small"]
    n_medium = size._to() // _DIVISORS["join_medium"]
    n_big = size._to() // _DIVISORS["join_big"]
    join_small = JoinSmallGenerator(size._to(), n_small, k, generation_seed, keys_seed, min([batch_size, n_small]))
    join_medium = JoinMediumGenerator(size._to(), n_medium, k, generation_seed, keys_seed, min([batch_size, n_medium]))
    join_big = JoinBigGenerator(size._to(), n_big, k, nas, generation_seed, keys_seed, min([batch_size, n_big]))

    data_filename_small = _create_filename("join_small", size._to().value, k, nas, data_format)
    data_filename_medium = _create_filename("join_medium", size._to().value, k, nas, data_format)
    data_filename_big = _create_filename("join_big", size._to().value, k, nas, data_format)

    output_dir = Path(path_prefix)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    output_small = output_dir.joinpath(data_filename_small)
    output_medium = output_dir.joinpath(data_filename_medium)
    output_big = output_dir.joinpath(data_filename_big)

    _clear_prev_if_exists(output_small, data_format)
    _clear_prev_if_exists(output_medium, data_format)
    _clear_prev_if_exists(output_big, data_format)

    print(
        f"{size._to().value // _DIVISORS['join_small']} rows will be saved into: [green]{output_small.absolute().__str__()}[/green]\n"
    )
    print(
        f"{size._to().value // _DIVISORS['join_medium']} rows will be saved into: [green]{output_medium.absolute().__str__()}[/green]\n"
    )
    print(
        f"{size._to().value // _DIVISORS['join_big']} rows will be saved into: [green]{output_big.absolute().__str__()}[/green]\n"
    )

    schema_small = pa.schema(
        [
            ("id1", pa.int64(), False),
            ("id4", pa.utf8(), False),
            ("v2", pa.float64(), False),
        ]
    )
    schema_medium = pa.schema(
        [
            ("id1", pa.int64(), False),
            ("id2", pa.int64(), False),
            ("id4", pa.utf8(), False),
            ("id5", pa.utf8(), False),
            ("v2", pa.float64(), False),
        ]
    )
    schema_big = pa.schema(
        [
            ("id1", pa.int64()),
            ("id2", pa.int64()),
            ("id3", pa.int64()),
            ("id4", pa.utf8(), False),
            ("id5", pa.utf8(), False),
            ("id6", pa.utf8(), False),
            ("v2", pa.float64()),
        ]
    )
    data_format.pprint()
    print()

    print("An [bold]SMALL[/bold] data [green]schema[/green] is the following:")
    print(schema_small)
    print()

    if data_format is Format.CSV:
        writer = csv.CSVWriter(sink=output_small, schema=schema_small)
        for batch in track(join_small.iter_batches(), total=len(join_small.batches)):
            writer.write_batch(batch)

        writer.close()

    if data_format is Format.PARQUET:
        writer = parquet.ParquetWriter(where=output_small, schema=schema_small)
        for batch in track(join_small.iter_batches(), total=len(join_small.batches)):
            writer.write_batch(batch)

        writer.close()

    if data_format is Format.DELTA:
        output_small.mkdir(parents=True)
        delta_file_pq = output_small.joinpath("data.parquet")
        writer = parquet.ParquetWriter(where=delta_file_pq, schema=schema_small)
        for batch in track(join_small.iter_batches(), total=len(join_small.batches)):
            writer.write_batch(batch)

        writer.close()
        generate_delta_log(output_small, schema_small)

    print()
    print("An [bold]MEDIUM[/bold] data [green]schema[/green] is the following:")
    print(schema_medium)
    print()

    if data_format is Format.CSV:
        writer = csv.CSVWriter(sink=output_medium, schema=schema_medium)
        for batch in track(join_medium.iter_batches(), total=len(join_medium.batches)):
            writer.write_batch(batch)

        writer.close()

    if data_format is Format.PARQUET:
        writer = parquet.ParquetWriter(where=output_medium, schema=schema_medium)
        for batch in track(join_medium.iter_batches(), total=len(join_medium.batches)):
            writer.write_batch(batch)

        writer.close()

    if data_format is Format.DELTA:
        output_medium.mkdir(parents=True)
        delta_file_pq = output_medium.joinpath("data.parquet")
        writer = parquet.ParquetWriter(where=delta_file_pq, schema=schema_medium)
        for batch in track(join_medium.iter_batches(), total=len(join_medium.batches)):
            writer.write_batch(batch)

        writer.close()
        generate_delta_log(output_medium, schema_medium)

    print()
    print("An [bold]BIG[/bold] data [green]schema[/green] is the following:")
    print(schema_big)
    print()

    if data_format is Format.CSV:
        writer = csv.CSVWriter(sink=output_big, schema=schema_big)
        for batch in track(join_big.iter_batches(), total=len(join_big.batches)):
            writer.write_batch(batch)

        writer.close()

    if data_format is Format.PARQUET:
        writer = parquet.ParquetWriter(where=output_big, schema=schema_big)
        for batch in track(join_big.iter_batches(), total=len(join_big.batches)):
            writer.write_batch(batch)

        writer.close()

    if data_format is Format.DELTA:
        output_big.mkdir(parents=True)
        delta_file_pq = output_big.joinpath("data.parquet")
        writer = parquet.ParquetWriter(where=delta_file_pq, schema=schema_big)
        for batch in track(join_big.iter_batches(), total=len(join_big.batches)):
            writer.write_batch(batch)

        writer.close()
        generate_delta_log(output_big, schema_big)


def entry_point() -> None:
    app()


if __name__ == "__main__":
    entry_point()
