import pytest
from datetime import date

@pytest.fixture
def mock_config():
    return {
        'base_url': 'https://example.com',
        'start_date': date(2023, 1, 1),
        'end_date': date(2023, 12, 31),
        'download_dir': '/tmp/downloads'
    }

@pytest.fixture
def parser(mock_config):
    from core.parser import SpimexParser
    return SpimexParser(config=mock_config)