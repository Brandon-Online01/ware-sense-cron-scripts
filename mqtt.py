import asyncio
import aio_pika
import aiomysql
import json
import datetime
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
DB_POOL_CONFIG = {
    'host': '102.67.136.195',
    'user': 'waresense',
    'password': '@waresense2024',
    'db': 'ware-sense',
    'minsize': 1,
    'maxsize': 100,  # Adjust this based on your needs
}

# RabbitMQ configuration
RABBITMQ_CONFIG = {
    'host': 'mqtt.savvyiot.co.za',  # Remove 'https://' as it's not needed for AMQP
    'port': 5672,
    'virtualhost': 'legend',
    'login': 'legend',
    'password': 'YvLC2tZhu5e6fafRmT4z'
}

# Global variables for message counting and rate calculation
message_count = 0
current_servicing_rate = 0
message_count_lock = asyncio.Lock()
servicing_rate_lock = asyncio.Lock()

# Async connection pool for MySQL
mysql_pool = None


async def init_db_pool():
    global mysql_pool
    mysql_pool = await aiomysql.create_pool(**DB_POOL_CONFIG)


async def handle_message(message: aio_pika.IncomingMessage):
    global message_count
    async with message.process():
        try:
            # Decode and parse the message body
            message_body = json.loads(message.body.decode('utf-8'))

            # Extract relevant information
            data = message_body.get('data', [])
            timestamp = message_body.get('timestamp')
            rssi = message_body.get('rssi')

            # Extract routing key information
            routing_key = message.routing_key
            routing_key_parts = routing_key.split('.')
            owner_tag = routing_key_parts[0] if len(
                routing_key_parts) > 0 else None
            project_name = routing_key_parts[1] if len(
                routing_key_parts) > 1 else None
            machine_mac_address = routing_key_parts[2] if len(
                routing_key_parts) > 2 else None
            type_value = routing_key_parts[3] if len(
                routing_key_parts) > 3 else None

            # Extract data for MySQL insertion
            for item in data:
                cycle_time = item.get('cycle_time', 0)
                cycle_completed_timestamp = item.get(
                    'cycle_completed_timestamp', 0)
                await insert_data_to_mysql(
                    cycle_time, cycle_completed_timestamp, timestamp, rssi,
                    message.delivery_tag, message.exchange, routing_key,
                    owner_tag, project_name, machine_mac_address, type_value
                )

            # Increment message count
            async with message_count_lock:
                message_count += 1

        except json.JSONDecodeError:
            logger.warning("Received message with invalid JSON format")
        except Exception as e:
            logger.error(f"Error processing message: {e}")


async def insert_data_to_mysql(cycle_time, cycle_completed_timestamp, event_timestamp, signal_strength,
                               delivery_tag, exchange_type, routing_key, owner_tag, project_name,
                               machine_mac_address, type_value):
    async with mysql_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            try:
                # Convert timestamps and values for insertion
                cycle_time_seconds = cycle_time / 1000 if cycle_time else 0
                cycle_completed_human_time = datetime.datetime.fromtimestamp(cycle_completed_timestamp, tz=datetime.timezone(datetime.timedelta(hours=2))).strftime(
                    '%a %b %d %Y %H:%M:%S GMT+0200 (South Africa Standard Time)') if cycle_completed_timestamp else 'none'
                event_human_time = datetime.datetime.fromtimestamp(
                    event_timestamp, tz=datetime.timezone(datetime.timedelta(hours=2))).strftime('%a %b %d %Y %H:%M:%S GMT+0200 (South Africa Standard Time)')

                query = """
                INSERT INTO queue_data (
                    cycle_time, cycle_completed_timestamp, eventTimeStamp, signalStrength,
                    deliveryTag, exchangeType, routingKey, ownerTag, projectName,
                    machineMacAddress, type, servicingRate
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                # Get the current servicing rate
                async with servicing_rate_lock:
                    current_rate = current_servicing_rate

                values = (
                    cycle_time_seconds, cycle_completed_human_time, event_human_time, signal_strength,
                    delivery_tag, exchange_type, routing_key, owner_tag, project_name,
                    machine_mac_address, type_value, current_rate
                )

                await cursor.execute(query, values)
                await conn.commit()

            except aiomysql.MySQLError as e:
                logger.error(f"Error inserting data into MySQL: {e}")


async def consume_messages():
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
                asyncio.create_task(handle_message(message))


async def calculate_servicing_rate():
    global message_count, current_servicing_rate
    last_count = 0
    while True:
        await asyncio.sleep(1)
        async with message_count_lock:
            current_count = message_count
            messages_per_second = current_count - last_count
            last_count = current_count

        async with servicing_rate_lock:
            current_servicing_rate = messages_per_second

        logger.info(f"Servicing Rate: {messages_per_second} msg/s")


async def main():
    await init_db_pool()

    # Start consumers and rate calculators
    await asyncio.gather(
        consume_messages(),
        calculate_servicing_rate()
    )

if __name__ == '__main__':
    asyncio.run(main())
