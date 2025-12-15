"""
Job2 tests
"""
import pytest
import sys
import json
from pathlib import Path
from unittest.mock import patch
from fastavro import reader

# Add project root to the sys.path
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


@pytest.fixture
def sample_data():
    return [
        {"client": "Test Client", "purchase_date": "2022-08-09",
         "product": "Test Product", "price": 100}
    ]


@pytest.fixture
def raw_dir_with_json(tmp_path, sample_data):
    """Creating raw directory with JSON file"""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    json_file = raw_dir / "sales_2022-08-09.json"
    with open(json_file, 'w') as f:
        json.dump(sample_data, f)

    return str(raw_dir)


class TestJob2:
    """Job2 tests: JSON to Avro convertation"""

    def test_convert_to_avro(self, raw_dir_with_json, tmp_path, sample_data):

        from lec02.job2.bll.sales_converter import convert_json_to_avro

        stg_dir = str(tmp_path / "stg")
        convert_json_to_avro(raw_dir_with_json, stg_dir)

        avro_file = Path(stg_dir) / "sales_2022-08-09.avro"
        assert avro_file.exists()

        with open(avro_file, 'rb') as f:
            records = list(reader(f))
        assert records == sample_data

    def test_convert_missing_raw_dir(self, tmp_path):
        """Test unexisting directory"""
        from lec02.job2.bll.sales_converter import convert_json_to_avro

        with pytest.raises(ValueError, match="Raw directory does not exist"):
            convert_json_to_avro("/nonexistent", str(tmp_path / "stg"))

    def test_convert_idempotency(self, raw_dir_with_json, tmp_path):
        """Idempotency test"""
        from lec02.job2.bll.sales_converter import convert_json_to_avro

        stg_dir = str(tmp_path / "stg")

        # first run
        convert_json_to_avro(raw_dir_with_json, stg_dir)
        # second run
        convert_json_to_avro(raw_dir_with_json, stg_dir)

        avro_file = Path(stg_dir) / "sales_2022-08-09.avro"
        assert avro_file.exists()

    @patch('lec02.job2.bll.sales_converter.convert_json_to_avro')
    def test_flask_endpoint(self, mock_convert):
        """Test Flask endpoint"""
        from lec02.job2 import main

        main.app.config['TESTING'] = True
        client = main.app.test_client()

        response = client.post('/', json={
            "raw_dir": "/path/to/raw",
            "stg_dir": "/path/to/stg"
        })

        assert response.status_code == 201
        mock_convert.assert_called_once_with(raw_dir="/path/to/raw", stg_dir="/path/to/stg")