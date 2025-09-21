import asyncio
import json
import logging
from typing import Optional, Callable
import sys
import aio_pika
from aio_pika import Message
from src.settings import settings

# Basic configuration
logging.basicConfig(
    level=logging.INFO,  # or DEBUG if you want more details
    stream=sys.stdout,
    format="%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Get a logger for the current module
logger = logging.getLogger(__name__)



class RabbitMQInterface:
    """
    A robust RabbitMQ interface with proper error handling and reconnection logic
    """

    def __init__(self):
        self.connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
        self.channel: Optional[aio_pika.abc.AbstractRobustChannel] = None
        self._connection_url = f"amqp://{settings.RABBITMQ_USER}:{settings.RABBITMQ_PASS}@{settings.RABBITMQ_HOST}/"

    async def create_connection(self, max_retries: int = 5, retry_delay: int = 3):
        """
        Connection to the RabbitMQ service with retry logic
        :param max_retries:
        :param retry_delay:
        :return:
        """
        logger.debug(f"Attempting to connect to RabbitMQ at {settings.RABBITMQ_HOST}")
        last_exception = None

        for attempt in range(1, max_retries + 1):
            try:
                logger.debug(f"Connection attempt {attempt}/{max_retries}")

                self.connection = await aio_pika.connect_robust(self._connection_url)
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=1)

                logger.debug("Successfully connected to RabbitMQ!")
                return

            except Exception as e:
                last_exception = e
                logger.error(f"Attempt {attempt}/{max_retries} failed: {e}")

                if attempt < max_retries:
                    logger.debug(f"Retrying in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"All {max_retries} connection attempts failed")

        # If we reach here, all attempts failed
        error_msg = f"Failed to connect to RabbitMQ after {max_retries} attempts"
        if last_exception:
            error_msg += f". Last error: {last_exception}"

        raise ConnectionError(error_msg)

    async def ensure_connection(self):
        """Ensure we have a valid connection and channel"""
        if not self.connection or self.connection.is_closed:
            await self.create_connection()

        if not self.channel or self.channel.is_closed:
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)

    async def declare_queue(self, q_name: str, durable: bool = True, exclusive: bool = False):
        """
        Create or ensure existence of a queue
        :param q_name: queue name
        :param durable: Whether the queue survives broker restarts
        :param exclusive: Whether the queue is exclusive to this connection
        """
        await self.ensure_connection()

        try:
            queue = await self.channel.declare_queue(
                q_name,
                durable=durable,
                exclusive=exclusive
            )
            logger.debug(f"Queue '{q_name}' declared successfully")
            return queue
        except Exception as e:
            logger.error(f"Failed to declare queue '{q_name}': {e}")
            raise

    async def publish(self, message: dict, q_name: str, routing_key: Optional[str] = None) -> None:
        """
        Publish a message to a queue
        :param message:
        :param q_name:
        :param routing_key:
        :return:
        """
        await self.ensure_connection()

        try:
            queue = await self.declare_queue(q_name)
            message_body = json.dumps(message, default=str).encode()

            amqp_message = Message(
                message_body,
                delivery_mode=2,  # Make message persistent
                timestamp=asyncio.get_event_loop().time()
            )

            # Publish to default exchange with queue name as routing key
            await self.channel.default_exchange.publish(
                amqp_message,
                routing_key=routing_key or queue.name
            )

            logger.debug(f"Message published to queue '{q_name}'")

        except Exception as e:
            logger.error(f"Failed to publish message to queue '{q_name}': {e}")
            raise

    async def consume(self, callback: Callable, q_name: str, auto_ack: bool = False):
        """
        Consumes messages from a queue and runs callback on it.
        :param callback:
        :param q_name:
        :param auto_ack:
        :return:
        """
        await self.ensure_connection()

        try:
            queue = await self.declare_queue(q_name)

            logger.debug(f"Starting to consume from queue '{q_name}'")
            await queue.consume(callback, no_ack=auto_ack)

            logger.debug(f"Consumer started for queue '{q_name}'. Waiting for messages...")
            await asyncio.Future()

        except Exception as e:
            logger.error(f"Failed to consume from queue '{q_name}': {e}")
            raise

    async def close(self):
        """Close the connection gracefully"""
        try:
            if self.channel and not self.channel.is_closed:
                await self.channel.close()
                logger.debug("Channel closed")

            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                logger.debug("Connection closed")

        except Exception as e:
            logger.error(f"Error closing RabbitMQ connection: {e}")
