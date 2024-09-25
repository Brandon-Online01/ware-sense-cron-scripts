import pytz
import json
import logging
import asyncio
import aio_pika
import aiomysql
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# RabbitMQ configurations
RABBITMQ_CONFIG = {
    'host': 'mqtt.savvyiot.co.za',
    'port': 5672,
    'virtualhost': 'legend',
    'login': 'legend',
    'password': 'YvLC2tZhu5e6fafRmT4z'
}

# Database configuration
DB_POOL_CONFIG = {
    'host': '102.67.136.195',
    'user': 'waresense',
    'password': '@waresense2024',
    'db': 'ware-sense',
    'minsize': 1,
    'maxsize': 100,  # Adjust this based on your needs
}


async def save_data(pool, cleaned_data):  # Pass pool as an argument
    try:
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
                # Define the SQL insert statement
                insert_query = """
                INSERT INTO queue_data (routingKey, deliveryTag, exchangeType, ownerTag, projectName,
                                              machineMacAddress, type, cycle_time, eventTimeStamp,
                                              signalStrength, cycle_completed_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                # Execute the insert statement with cleaned data
                await cursor.execute(insert_query, (
                    cleaned_data['routingKey'],
                    cleaned_data['deliveryTag'],
                    cleaned_data['exchangeType'],
                    cleaned_data['ownerTag'],
                    cleaned_data['projectName'],
                    cleaned_data['machineMacAddress'],
                    cleaned_data['type'],
                    cleaned_data['cycle_time'],
                    cleaned_data['eventTimeStamp'],
                    cleaned_data['signalStrength'],
                    cleaned_data['cycle_completed_timestamp']
                ))
                await connection.commit()  # Commit the transaction
                logger.info(
                    f"Data saved to the database successfully: {cleaned_data}")
    except Exception as e:
        logger.error(f"Failed to save data to the database: {e}")


# Accept pool as an argument
async def message_processor(message: aio_pika.IncomingMessage, pool):
    try:
        # Decode and parse the message body
        message_body = json.loads(message.body.decode('utf-8'))

        # Clean and extract relevant data from the message body
        data_entries = message_body.get('data', [])
        timestamp = message_body.get('timestamp')
        rssi = message_body.get('rssi')

        # Convert epoch timestamp to human-readable format with timezone
        local_tz = pytz.timezone('Africa/Johannesburg')
        human_readable_time = datetime.fromtimestamp(timestamp, local_tz).strftime(
            '%a %b %d %Y %H:%M:%S GMT%z (South Africa Standard Time)')

        # Extract additional fields from the message
        cleaned_data = {
            'routingKey': message.consumer_tag,
            'deliveryTag': message.delivery_tag,
            'exchangeType': message.exchange,

            'routingKey': message.routing_key,
            # First part of routing key
            'ownerTag': message.routing_key.split('.')[0],
            # Second part of routing key
            'projectName': message.routing_key.split('.')[1] if len(message.routing_key.split('.')) > 1 else None,
            # Third part of routing key
            'machineMacAddress': message.routing_key.split('.')[2] if len(message.routing_key.split('.')) > 2 else None,
            # Third part of routing key
            'type': message.routing_key.split('.')[3] if len(message.routing_key.split('.')) > 3 else None
        }

        for entry in data_entries:
            entry_value = entry.get('value')

            # Append cycleTime to cleaned data (convert to seconds)
            cleaned_data['cycle_time'] = entry_value / \
                1000  # Convert milliseconds to seconds
            # Use human-readable timestamp
            cleaned_data['eventTimeStamp'] = human_readable_time
            # Rename rssi to signalStrength
            cleaned_data['signalStrength'] = rssi
            # Append cycle_completed_timestamp to cleaned data
            cleaned_data['cycle_completed_timestamp'] = human_readable_time

            await save_data(pool, cleaned_data)

        # Acknowledge the message after saving the data
        await message.ack()  # Acknowledge the message

    except json.JSONDecodeError:
        logger.warning("Received message with invalid JSON format")
        await message.nack()  # Optionally, you can nack the message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        await message.nack()  # Optionally, you can nack the message


async def consume_messages(pool):  # Accept pool as an argument
    connection = await aio_pika.connect_robust(
        host=RABBITMQ_CONFIG['host'],
        port=RABBITMQ_CONFIG['port'],
        virtualhost=RABBITMQ_CONFIG['virtualhost'],
        login=RABBITMQ_CONFIG['login'],
        password=RABBITMQ_CONFIG['password']
    )

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1000)

        queue = await channel.declare_queue('mqtt', durable=True)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                # Process the message
                await message_processor(message, pool)


async def main():
    # Create database connection pool
    db_pool = await aiomysql.create_pool(
        host=DB_POOL_CONFIG['host'],
        user=DB_POOL_CONFIG['user'],
        password=DB_POOL_CONFIG['password'],
        db=DB_POOL_CONFIG['db'],
        minsize=DB_POOL_CONFIG['minsize'],
        maxsize=DB_POOL_CONFIG['maxsize']
    )

    if db_pool:
        # Start consuming messages
        await consume_messages(db_pool)  # Pass db_pool to consume_messages
    else:
        logger.error("Exiting due to database connection failure.")


if __name__ == '__main__':
    asyncio.run(main())
