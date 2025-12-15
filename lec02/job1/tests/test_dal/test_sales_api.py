"""
sales_api.py tests
"""
import pytest
import sys
from pathlib import Path
from unittest.mock import Mock, patch
import os

job1_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(job1_path))

os.environ['API_AUTH_TOKEN'] = 'test_token'


@pytest.fixture
def sample_data():
    return [
        {"client": "Test", "purchase_date": "2022-08-09",
         "product": "Product", "price": 100}
    ]


class TestGetSales:

    @patch('dal.sales_api.requests.get')
    def test_get_sales_success(self, mock_get, sample_data):
        from dal.sales_api import get_sales

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_data

        mock_get.side_effect = [mock_response, Mock(status_code=404)]

        result = get_sales("2022-08-09")
        assert result == sample_data
        assert mock_get.call_count == 2

    @patch('dal.sales_api.requests.get')
    def test_get_sales_auth_error(self, mock_get):
        from dal.sales_api import get_sales

        mock_response = Mock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="Invalid AUTH_TOKEN"):
            get_sales("2022-08-09")