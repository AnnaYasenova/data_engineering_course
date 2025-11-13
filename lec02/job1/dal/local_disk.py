from typing import List, Dict, Any
from pathlib import Path
import shutil, json

def save_to_disk(json_content: List[Dict[str, Any]], path: str, date: str) -> None:
    """
        Saves  sales data to local disk.

        Args:
            json_content: sales data
            date: Date in format YYYY-MM-DD (e.g., "2022-08-09")
            path: Path to directory where raw data should be saved
        """

    raw_path = Path(path)

    if raw_path.exists():
        shutil.rmtree(raw_path)

    raw_path.mkdir(parents=True, exist_ok=True)

    output_file = raw_path / f"sales_{date}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(json_content, f, indent=2, ensure_ascii=False)

    print(f"Successfully saved {len(json_content)} records to {output_file}")