# RabbitMQ to MySQL Data Pipeline

A high-performance asynchronous data pipeline built with [Python](https://www.python.org/) that consumes messages from [RabbitMQ](https://www.rabbitmq.com/) and stores them in a [MySQL](https://www.mysql.com/) database. The pipeline leverages connection pooling and asynchronous processing for optimal throughput.

![Python](https://img.shields.io/badge/python-3.7+-blue.svg)
![RabbitMQ](https://img.shields.io/badge/rabbitmq-latest-orange.svg)
![MySQL](https://img.shields.io/badge/mysql-latest-blue.svg)

## Features

- Asynchronous message consumption from RabbitMQ using `aio_pika`
- Efficient database operations with MySQL connection pooling via `aiomysql`
- Robust error handling and logging
- Environment-based configuration using `.env` files
- Support for message acknowledgments and requeuing

## Requirements

- Python 3.7+
- RabbitMQ Server
- MySQL Server
- Required Python packages:
  - aio_pika
  - aiomysql
  - python-dotenv
  - pytz

## Configuration

The application uses environment variables for configuration. Create a `.env` file in the project root with:

```ini
# RabbitMQ Configuration
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_VHOST=/
RABBITMQ_LOGIN=guest
RABBITMQ_PASSWORD=guest

# Database Configuration
DB_HOST=localhost
DB_USER=your_user
DB_PASSWORD=your_password
DB_NAME=your_database
DB_MIN_CONNECTIONS=1
DB_MAX_CONNECTIONS=1000
```

## Project Structure

- `app.py`: Main application entry point
- `config/settings.py`: Configuration management
- `messaging/processor.py`: Message processing logic
- `database/operations.py`: Database operations

## Main Components

1. **Message Consumer** (`app.py`):
   - Establishes RabbitMQ connection
   - Sets up message queue with prefetch count
   - Manages database connection pool

2. **Message Processor** (`messaging/processor.py`):
   - Parses incoming messages
   - Transforms data into the required format
   - Handles message acknowledgments

3. **Database Operations** (`database/operations.py`):
   - Manages database write operations
   - Implements connection pooling
   - Handles database errors

## Installation

### Setting Up Virtual Environment (MacOS)

1. Create and activate a virtual environment in one command:
```bash
python3 -m venv .venv && source .venv/bin/activate && python3 -m pip install --upgrade pip
```

2. Install required packages:
```bash
python3 -m pip install aio_pika aiomysql python-dotenv pytz
```

3. Run the application:
```bash
python3 app.py
```

### Alternative Setup Methods

If you prefer to set up the environment step by step:

1. Create a virtual environment:
```bash
python3 -m venv .venv
```

2. Activate the virtual environment:
```bash
source .venv/bin/activate
```

3. Upgrade pip and install packages:
```bash
python3 -m pip install --upgrade pip
python3 -m pip install aio_pika aiomysql python-dotenv pytz
```

4. Run the application:
```bash
python3 app.py
```

Note: The virtual environment directory (`.venv`) should be added to your `.gitignore` file to keep it out of version control.

## Usage

1. Ensure RabbitMQ and MySQL servers are running
2. Configure your `.env` file
3. Run the application:
```bash
python3 app.py
```

The script will start consuming messages from RabbitMQ and storing them in MySQL.

## Message Format

The pipeline expects messages in the following JSON format:
```json
{
    "data": [...],
    "timestamp": 1234567890,
    "rssi": -70,
    "firmware_version": "1.0.0"
}
```

## Error Handling

- Invalid JSON messages are logged and requeued
- Failed database operations trigger message requeuing
- All errors are logged with appropriate context

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[MIT](https://choosealicense.com/licenses/mit/)
