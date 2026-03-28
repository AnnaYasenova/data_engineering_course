"""
Tests for Flask endpoint Job1
"""
import pytest
import sys
import os
from pathlib import Path
from unittest.mock import patch

# Configure PYTHONPATH before imports
job1_root = Path(__file__).parent.parent
if str(job1_root) not in sys.path:
    sys.path.insert(0, str(job1_root))


class TestJob1Main:
    """Tests for Flask API Job1"""

    @patch('bll.sales_api.save_sales_to_local_disk')
    def test_flask_endpoint_success(self, mock_save):
        """POST endpoint test success"""
        import main

        main.app.config['TESTING'] = True
        client = main.app.test_client()

        response = client.post('/', json={
            "date": "2022-08-09",
            "raw_dir": "/path/to/raw"
        })

        assert response.status_code == 201
        mock_save.assert_called_once_with(date="2022-08-09", raw_dir="/path/to/raw")

    def test_missing_date(self):
        """Missing date parameter check"""
        import main

        main.app.config['TESTING'] = True
        client = main.app.test_client()

        response = client.post('/', json={
            "raw_dir": "/path/to/raw"
        })

        assert response.status_code == 400

    def test_missing_raw_dir(self):
        """Missing raw_dir parameter check"""
        import main

        main.app.config['TESTING'] = True
        client = main.app.test_client()

        response = client.post('/', json={
            "date": "2022-08-09"
        })

        assert response.status_code == 400