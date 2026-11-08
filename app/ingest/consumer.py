import asyncio
import logging

import aio_pika

from app.config import settings
from app.db import Cols

log = logging.getLogger("ingest")


async def pump(connection, queue_name: str, handler, cols: Cols):
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=20)
    queue = await channel.declare_queue(queue_name, passive=True)
    log.info("Consuming from %s", queue_name)
    async with queue.iterator() as it:
        async for message in it:
            await handler(message, cols)


async def run_queue(queue_name: str, handler, cols: Cols):
    while True:
        try:
            connection = await aio_pika.connect_robust(settings.amqp_url)
            async with connection:
                await pump(connection, queue_name, handler, cols)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Queue %s: consumer stopped, retrying in 5 s", queue_name)
        await asyncio.sleep(5)
