import os
import logging

from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('spimex_parser.log')
    ]
)
logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent.parent
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

ASYNC_DB_CONFIG = {
    'database': os.getenv('ASYNC_DB_DATABASE'),
    'user': os.getenv('ASYNC_DB_USER'),
    'password': os.getenv('ASYNC_DB_PASSWORD'),
    'host': os.getenv('ASYNC_DB_HOST'),
    'port': os.getenv('ASYNC_DB_PORT')
}
PARSER_CONFIG = {
    'base_url': "https://spimex.com/markets/oil_products/trades/results/",
    'download_dir': os.path.join(BASE_DIR, "downloads"),
    'start_date': datetime(2025, 3, 1),
    'end_date': datetime.now()
}

print(ASYNC_DB_CONFIG)