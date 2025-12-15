"""
local_disk.py tests
"""
import pytest
import sys
import json
from pathlib import Path

# Add path to job1
job1_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(job1_path))


@pytest.fixture
def sample_data():
    return [
        {"client": "Test", "purchase_date": "2022-08-09",
         "product": "Product", "price": 100}
    ]


class TestLocalDisk:

    def test_save_to_disk_creates_file(self, sample_data, tmp_path):
        from dal.local_disk import save_to_disk

        raw_dir = str(tmp_path / "raw")
        save_to_disk(sample_data, raw_dir, "2022-08-09")

        output_file = Path(raw_dir) / "sales_2022-08-09.json"
        assert output_file.exists()

        with open(output_file, 'r') as f:
            saved_data = json.load(f)
        assert saved_data == sample_data

    def test_save_to_disk_idempotency(self, sample_data, tmp_path):
        from dal.local_disk import save_to_disk

        raw_dir = str(tmp_path / "raw")
        save_to_disk(sample_data, raw_dir, "2022-08-09")

        new_data = [{"client": "New", "purchase_date": "2022-08-09",
                     "product": "New", "price": 999}]
        save_to_disk(new_data, raw_dir, "2022-08-09")

        output_file = Path(raw_dir) / "sales_2022-08-09.json"
        with open(output_file, 'r') as f:
            saved_data = json.load(f)

        assert saved_data == new_data