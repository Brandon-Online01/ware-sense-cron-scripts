import logging
import asyncio
import aio_pika
import aiomysql
from config.settings import RABBITMQ_CONFIG, DB_POOL_CONFIG
from messaging.processor import message_processor

logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def consume_messages(pool):
    logger.info("connected...")
    connection = await aio_pika.connect_robust(
        host=RABBITMQ_CONFIG['host'],
        port=RABBITMQ_CONFIG['port'],
        virtualhost=RABBITMQ_CONFIG['virtualhost'],
        login=RABBITMQ_CONFIG['login'],
        password=RABBITMQ_CONFIG['password']
    )

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=10)
        queue = await channel.declare_queue('mqtt', durable=True)
        logger.info("Queue declared, waiting for messages...")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                await message_processor(message, pool)

async def main():
    db_pool = await aiomysql.create_pool(**DB_POOL_CONFIG)

    if db_pool:
        await consume_messages(db_pool)
    else:
        logger.error("Exiting due to database connection failure.")

if __name__ == '__main__':
    asyncio.run(main())