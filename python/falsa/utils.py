from __future__ import annotations

import json
import time
from pathlib import Path
from uuid import uuid4

from pyarrow import Schema

PA_2_DELTA_DTYPES = {
    "int32": "integer",
    "int64": "long",
}


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
