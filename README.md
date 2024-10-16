# RabbitMQ to MySQL Data Pipeline

This Python script implements an asynchronous data pipeline that consumes messages from RabbitMQ and stores them in a MySQL database. It's designed to handle high-throughput scenarios and provides real-time monitoring of the message processing rate.

## Features

-   Asynchronous message consumption from RabbitMQ
-   Efficient data insertion into MySQL using connection pooling
-   Real-time calculation and logging of message processing rate
-   Error handling and logging

## Requirements

-   Python 3.7+
-   aio_pika
-   aiomysql
-   asyncio

## Configuration

The script uses the following configurations:

1. Database configuration (MySQL):

    - Host, user, password, database name, and connection pool settings

2. RabbitMQ configuration:
    - Host, port, virtual host, login, and password

Ensure these configurations are set correctly before running the script.

## Main Components

1. `init_db_pool()`: Initializes the MySQL connection pool
2. `handle_message()`: Processes incoming RabbitMQ messages
3. `insert_data_to_mysql()`: Inserts parsed data into the MySQL database
4. `consume_messages()`: Sets up the RabbitMQ consumer
5. `calculate_servicing_rate()`: Calculates and logs the message processing rate
6. `main()`: Orchestrates the entire process

## Usage

Run the script using Python:

```
python app.py
```

The script will start consuming messages from RabbitMQ, process them, and store the data in MySQL. It will also log the current processing rate every second.


Mac OS for setting up a virtual environment:
## Setting Up a Virtual Environment

To set up a Python virtual environment, run the following commands:

```
python3 -m venv path/to/venv && source path/to/venv/bin/activate && python3 app.py


to install the required packages, run the following command:
python3 -m pip install xyz


to run the script, run the following command:
python3 app.py

```

Replace `path/to/venv` with your desired virtual environment path and `xyz` with the required packages.

## Note

Ensure that you have the necessary permissions and network access to connect to both RabbitMQ and MySQL servers as specified in the configuration.
