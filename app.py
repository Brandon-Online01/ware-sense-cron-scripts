import pytz
import json
import logging
import asyncio
import aio_pika
import aiomysql
from datetime import datetime

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RABBITMQ_CONFIG = {
    'host': 'mqtt.savvyiot.co.za',
    'port': 5672,
    'virtualhost': 'legend',
    'login': 'legend',
    'password': 'YvLC2tZhu5e6fafRmT4z'
}

DB_POOL_CONFIG = {
    'host': '102.67.136.195',
    'user': 'waresense',
    'password': 'waresense@2024',
    'db': 'ware-sense',
    'minsize': 1,
    'maxsize': 100,
}

# Async connection pool for MySQL
mysql_pool = None


async def init_db_pool():
    global mysql_pool
    mysql_pool = await aiomysql.create_pool(**DB_POOL_CONFIG)


async def write_to_db(pool, cleaned_data):
    async with pool.acquire() as connection:
        async with connection.cursor() as cursor:
            # Assuming a table named 'queue_data' with appropriate columns
            await cursor.execute("""
                INSERT INTO queue_data (routingKey, deliveryTag, exchangeType,
                                           ownerTag, projectName, machineMacAddress,
                                           type, cycleTime, eventTimeStamp,
                                           signalStrength, cycleCompletedTimestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                cleaned_data['routingKey'],
                cleaned_data['deliveryTag'],
                cleaned_data['exchangeType'],
                cleaned_data['ownerTag'],
                cleaned_data['projectName'],
                cleaned_data['machineMacAddress'],
                cleaned_data['type'],
                cleaned_data['cycleTime'],
                cleaned_data['eventTimeStamp'],
                cleaned_data['signalStrength'],
                cleaned_data['cycleCompletedTimestamp']
            ))

            await connection.commit()
            return True
    logger.error("Failed to write to database.")
    return False


async def message_processor(message: aio_pika.IncomingMessage, pool):
    try:
        message_body = json.loads(message.body.decode('utf-8'))
        data_entries = message_body.get('data', [])
        timestamp = message_body.get('timestamp')
        rssi = message_body.get('rssi')
        firmwareVersion = message_body.get('firmware_version')
        local_tz = pytz.timezone('Africa/Johannesburg')
        human_readable_time = datetime.fromtimestamp(timestamp, local_tz).strftime(
            '%a %b %d %Y %H:%M:%S GMT%z (South Africa Standard Time)')
        cleaned_data = {
            'routingKey': message.consumer_tag,
            'deliveryTag': message.delivery_tag,
            'exchangeType': message.exchange,
            'routingKey': message.routing_key,
            'ownerTag': message.routing_key.split('.')[0],
            'projectName': message.routing_key.split('.')[1] if len(message.routing_key.split('.')) > 1 else None,
            'machineMacAddress': message.routing_key.split('.')[2] if len(message.routing_key.split('.')) > 2 else None,
            'type': message.routing_key.split('.')[3] if len(message.routing_key.split('.')) > 3 else None,
            'eventTimeStamp': human_readable_time,
            'signalStrength': rssi,
            'cycleCompletedTimestamp': human_readable_time,
            'firmwareVersion': firmwareVersion,
            'cycleTime': 0,
        }

        for entry in data_entries:
            entry_value = entry.get('value')
            logger.info(f"Data type: {message.routing_key.split('.')[3]}")
            # Check if entry_value is a number and routing_key type is 'data'
            if isinstance(entry_value, (int, float)) and message.routing_key.split('.')[3] == 'data':
                cleaned_data['cycleTime'] = entry_value / 1000

            cleaned_data['eventTimeStamp'] = human_readable_time
            cleaned_data['signalStrength'] = rssi
            cleaned_data['cycleCompletedTimestamp'] = human_readable_time

        write_success = await write_to_db(pool, cleaned_data)

        if write_success:
            logger.info(
                f"DB: {message.delivery_tag}, - {cleaned_data['machineMacAddress']}")
            await message.ack()
        else:
            logger.error(f"Message failed to write to database")
            await message.nack(requeue=True)

    except json.JSONDecodeError:
        logger.warning("Received message with invalid JSON format")
        await message.nack()
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        await message.nack()


async def consume_messages(pool):
    logger.info("Starting to consume messages...")
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
        logger.info("Queue declared, waiting for messages...")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                await message_processor(message, pool)


async def main():
    await init_db_pool()

    if mysql_pool:
        await asyncio.gather(
            consume_messages(mysql_pool),
    )
    else:
        logger.error("DB connection unsuccesful.")

if __name__ == '__main__':
    asyncio.run(main())
