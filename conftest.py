"""
pytest configuration
"""
import pytest
import os
import sys
from pathlib import Path


@pytest.fixture(autouse=True)
def setup_env():
    """Set AUTH_TOKEN automatically for all tests"""
    os.environ['API_AUTH_TOKEN'] = 'test_token'
    yield


def pytest_configure(config):
    """Configure pytest - add paths for job1 and job2"""
    project_root = Path(__file__).parent

    # Додаємо шляхи до обох джоб
    job1_path = project_root / "lec02" / "job1"
    job2_path = project_root / "lec02" / "job2"

    if job1_path.exists():
        sys.path.insert(0, str(job1_path))
    if job2_path.exists():
        sys.path.insert(0, str(job2_path))