import json
import pytz
import logging
from datetime import datetime
from aio_pika import IncomingMessage
from database.operations import write_to_db

logger = logging.getLogger(__name__)

async def message_processor(message: IncomingMessage, pool):
    try:
        message_body = json.loads(message.body.decode('utf-8'))
        data_entries = message_body.get('data', [])
        timestamp = message_body.get('timestamp')
        rssi = message_body.get('rssi')
        firmwareVersion = message_body.get('firmware_version')
        
        local_tz = pytz.timezone('Africa/Johannesburg')
        human_readable_time = datetime.fromtimestamp(timestamp, local_tz).strftime(
            '%a %b %d %Y %H:%M:%S GMT%z (South Africa Standard Time)')
        
        routing_keys = message.routing_key.split('.')
        cleaned_data = {
            'routingKey': message.consumer_tag,
            'deliveryTag': message.delivery_tag,
            'exchangeType': message.exchange,
            'routingKey': message.routing_key,
            'ownerTag': routing_keys[0],
            'projectName': routing_keys[1] if len(routing_keys) > 1 else None,
            'machineMacAddress': routing_keys[2] if len(routing_keys) > 2 else None,
            'type': routing_keys[3] if len(routing_keys) > 3 else None,
            'eventTimeStamp': human_readable_time,
            'signalStrength': rssi,
            'cycleCompletedTimestamp': human_readable_time,
            'firmwareVersion': firmwareVersion,
            'cycleTime': 0,
        }

        for entry in data_entries:
            entry_value = entry.get('value')
            if isinstance(entry_value, (int, float)) and cleaned_data['type'] == 'data':
                cleaned_data['cycleTime'] = entry_value

        write_success = await write_to_db(pool, cleaned_data)

        if write_success:
            logger.info(f"DB: {cleaned_data['machineMacAddress']}")
            await message.ack()
        else:
            logger.error(f"Message failed to write to database")
            logger.info(f"NO-DB: {cleaned_data['machineMacAddress']}")
            await message.nack(requeue=True)

    except json.JSONDecodeError:
        logger.warning("Received message with invalid JSON format")
        await message.nack(requeue=True)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        await message.nack(requeue=True)