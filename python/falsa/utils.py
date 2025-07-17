from __future__ import annotations

import json
import shutil
import time
from enum import Enum
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
from pyarrow import Schema, csv, parquet
from rich import print

from falsa import H2ODatasetSizes

PA_2_DELTA_DTYPES = {
    "int32": "integer",
    "int64": "long",
}


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
DIVISORS = {
    "groupby": 1,
    "join_big": 1,
    "join_big_na": 1,
    "join_small": 1_000_000,
    "join_medium": 1_000,
    "join_lhs": 1,
}


class Schemas(Enum):
    GROUPBY = pa.schema(
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
    JOIN_LHS = pa.schema(
        [
            ("id1", pa.int64(), False),
            ("id2", pa.int64(), False),
            ("id3", pa.int64(), False),
            ("id4", pa.utf8(), False),
            ("id5", pa.utf8(), False),
            ("id6", pa.utf8(), False),
            ("v1", pa.float64(), False),
        ]
    )
    JOIN_RHS_SMALL = pa.schema(
        [
            ("id1", pa.int64(), False),
            ("id4", pa.utf8(), False),
            ("v2", pa.float64(), False),
        ]
    )
    JOIN_RHS_MEDIUM = pa.schema(
        [
            ("id1", pa.int64(), False),
            ("id2", pa.int64(), False),
            ("id4", pa.utf8(), False),
            ("id5", pa.utf8(), False),
            ("v2", pa.float64(), False),
        ]
    )
    JOIN_RHS_BIG = pa.schema(
        [
            ("id1", pa.int64(), False),
            ("id2", pa.int64(), False),
            ("id3", pa.int64(), False),
            ("id4", pa.utf8(), False),
            ("id5", pa.utf8(), False),
            ("id6", pa.utf8(), False),
            ("v2", pa.float64(), False),
        ]
    )


def create_filename(ds_type: str, n: int, k: int, nas: int, fmt: Format) -> str:
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
        "join_lhs": "J1_{n}_NA_{nas}{fmt}",
    }

    n_divisor = DIVISORS[ds_type]
    n_divided = n // n_divisor
    return output_names[ds_type].format(n=pretty_sci(n), n_divided=pretty_sci(n_divided), k=k, nas=nas, fmt=suffix)


def clear_prev_if_exists(fp: Path, fmt: Format) -> None:
    if fp.exists():
        # All is file, delta is directory
        if fmt is not Format.DELTA:
            fp.unlink()
        else:
            shutil.rmtree(fp, ignore_errors=True)


def get_writer(data_format: Format, schema: Schema, output_filepath: Path) -> csv.CSVWriter | parquet.ParquetWriter:
    data_format.pprint()
    print()

    if data_format is Format.CSV:
        return csv.CSVWriter(sink=output_filepath, schema=schema)
    elif data_format is Format.PARQUET:
        return parquet.ParquetWriter(where=output_filepath, schema=schema)
    else:
        delta_file_pq = output_filepath.joinpath("data.parquet")
        return parquet.ParquetWriter(where=delta_file_pq, schema=schema)


def pretty_sci(n: int) -> str:
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


def generate_delta_log(output_filepath: Path, schema: Schema) -> None:
    """Generate a delta-log from existing parquet files and the given schema."""
    file_len = 20
    delta_dir = output_filepath.joinpath("_delta_log")
    delta_dir.mkdir(exist_ok=True)
    add_meta_log = "0" * file_len + ".json"

    with open(delta_dir.joinpath(add_meta_log), "w") as meta_log:
        jsons = []
        # Generate "metaData"
        jsons.append(
            json.dumps(
                {
                    "metaData": {
                        "id": uuid4().__str__(),
                        "format": {
                            "provider": "parquet",
                            "options": {},
                        },
                        "schemaString": json.dumps(
                            {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": field.name,
                                        "type": PA_2_DELTA_DTYPES.get(field.type.__str__(), field.type.__str__()),
                                        "nullable": field.nullable,
                                        "metadata": {},
                                    }
                                    for field in schema
                                ],
                            }
                        ),
                        "configuration": {},
                        "partitionColumns": [],
                    }
                }
            )
        )
        # Generate "add"
        for pp in output_filepath.glob("*.parquet"):
            jsons.append(
                json.dumps(
                    {
                        "add": {
                            "path": pp.relative_to(output_filepath).__str__(),
                            "partitionValues": {},
                            "size": pp.stat().st_size,
                            "modificationTime": int(time.time() * 1000),
                            "dataChange": True,
                        }
                    }
                )
            )

        # Generate "protocol"
        jsons.append(json.dumps({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}))
        meta_log.write("\n".join(jsons))
