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

async def message_processor(message: aio_pika.IncomingMessage, pool):
    try:
        message_body = json.loads(message.body.decode('utf-8'))
        data_entries = message_body.get('data', [])
        timestamp = message_body.get('timestamp')
        rssi = message_body.get('rssi')
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
            'type': message.routing_key.split('.')[3] if len(message.routing_key.split('.')) > 3 else None
        }

        for entry in data_entries:
            entry_value = entry.get('value')
            cleaned_data['cycleTime'] = entry_value / 1000
            cleaned_data['eventTimeStamp'] = human_readable_time
            cleaned_data['signalStrength'] = rssi
            cleaned_data['cycleCompletedTimestamp'] = human_readable_time
            logger.info(f"DB: {cleaned_data}")

    except json.JSONDecodeError:
        logger.warning("Received message with invalid JSON format")
        await message.nack()
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        await message.nack()

async def consume_messages(pool):
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
                await message_processor(message, pool)

async def main():
    db_pool = await aiomysql.create_pool(
        host=DB_POOL_CONFIG['host'],
        user=DB_POOL_CONFIG['user'],
        password=DB_POOL_CONFIG['password'],
        db=DB_POOL_CONFIG['db'],
        minsize=DB_POOL_CONFIG['minsize'],
        maxsize=DB_POOL_CONFIG['maxsize']
    )

    if db_pool:
        await consume_messages(db_pool)
    else:
        logger.error("Exiting due to database connection failure.")

if __name__ == '__main__':
    asyncio.run(main())