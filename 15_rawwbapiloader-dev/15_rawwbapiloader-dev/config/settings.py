import os
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

ENVIRONMENT = os.getenv("ENVIRONMENT", "dev") # dev/prod
DEBUG = os.getenv("DEBUG", False) #Is_TestData == 0/1
BOT_NOTIFICATIONS = os.getenv("BOT_NOTIFICATIONS", False)
TLG_BOT_PATH = os.getenv('TLG_BOT_PATH')

ENV_CONFIG = {
        'dev': {
            'DB_SERVER': os.getenv("DB_SERVER"),
            'DB_NAME': os.getenv("DB_NAME"),
            'API_KEY': os.getenv('WB_API_KEY_DEV')
        },
        "prod": {
            'DB_SERVER': os.getenv("DB_SERVER", "PC108"),
            'DB_NAME': os.getenv("DB_NAME", "db_RawDataStore"),
            'API_KEY': os.getenv('WB_API_KEY_PROD')
        }
}

DB_DRIVER = os.getenv("DB_DRIVER","ODBC Driver 17 for SQL Server")
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
# Database timeouts (in seconds)
DB_QUERY_TIMEOUT = int(os.getenv('DB_QUERY_TIMEOUT', 120))  # seconds
DB_LOCK_TIMEOUT_MS = int(os.getenv('DB_LOCK_TIMEOUT_MS', 15000))  # ms
DB_LOGIN_TIMEOUT = int(os.getenv('DB_LOGIN_TIMEOUT', 5))  # seconds

SCENARIOS = {
    'wb17': {
        'description': 'Generate WB17 product catalog report',
        'required_args': {
            #"name" : "description",
            # ...
        },
    },
    'wb24': {
        'description': 'Generate WB24 products and prices catalog report',
        'required_args': {
            # "name" : "description",
            # ...
        },
    },
    #Add more scenarios here
}




