import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

async def write_to_db(pool, cleaned_data: Dict[str, Any]) -> bool:
    try:
        async with pool.acquire() as connection:
            async with connection.cursor() as cursor:
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
    except Exception as e:
        logger.error(f"Error writing to database: {e}")
        return False