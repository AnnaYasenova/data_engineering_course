"""
Business logic layer for converting JSON sales data to Avro format
"""
import json
import shutil
from pathlib import Path
from typing import List, Dict, Any
from fastavro import writer, parse_schema

SALES_SCHEMA = {
    "type": "record",
    "name": "Sale",
    "fields": [
        {"name": "client", "type": "string"},
        {"name": "purchase_date", "type": "string"},
        {"name": "product", "type": "string"},
        {"name": "price", "type": "int"}
    ]
}


def convert_json_to_avro(raw_dir: str, stg_dir: str) -> None:
    """
    Converts JSON files from raw_dir to Avro format in stg_dir.

    Args:
        raw_dir: Path to directory with raw JSON files
        stg_dir: Path to directory where Avro files should be saved
    """
    raw_path = Path(raw_dir)
    stg_path = Path(stg_dir)

    if not raw_path.exists():
        raise ValueError(f"Raw directory does not exist: {raw_dir}")

    if stg_path.exists():
        shutil.rmtree(stg_path)

    stg_path.mkdir(parents=True, exist_ok=True)

    json_files = list(raw_path.glob("sales_*.json"))

    if not json_files:
        raise ValueError(f"No JSON files found in {raw_dir}")

    parsed_schema = parse_schema(SALES_SCHEMA)

    for json_file in json_files:
        print(f"Processing {json_file.name}...")

        with open(json_file, 'r', encoding='utf-8') as f:
            sales_data: List[Dict[str, Any]] = json.load(f)

        avro_filename = json_file.stem + ".avro"
        avro_path = stg_path / avro_filename

        with open(avro_path, 'wb') as out:
            writer(out, parsed_schema, sales_data)

        print(f"Converted {len(sales_data)} records to {avro_filename}")

    print(f"Successfully converted {len(json_files)} file(s) to Avro format")