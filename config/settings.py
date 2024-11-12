import os
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_CONFIG = {
    'host': os.getenv('RABBITMQ_HOST'),
    'port': int(os.getenv('RABBITMQ_PORT', 5672)),
    'virtualhost': os.getenv('RABBITMQ_VHOST'),
    'login': os.getenv('RABBITMQ_LOGIN'),
    'password': os.getenv('RABBITMQ_PASSWORD')
}

DB_POOL_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'db': os.getenv('DB_NAME'),
    'minsize': int(os.getenv('DB_MIN_CONNECTIONS', 1)),
    'maxsize': int(os.getenv('DB_MAX_CONNECTIONS', 1000)),
}