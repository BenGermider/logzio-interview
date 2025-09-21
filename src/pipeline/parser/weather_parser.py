import json
import logging
import sys
from src.pipeline.infra.rabbitmq.rabbitmq_interface import RabbitMQInterface
from src.pipeline.parser.basic_parser import BasicParser
from src.settings import settings

# Basic configuration
logging.basicConfig(
    level=logging.INFO,  # or DEBUG if you want more details
    stream=sys.stdout,
    format="%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

class WeatherParser(BasicParser):

    def __init__(self, rmq_interface=RabbitMQInterface()):
        super().__init__(rmq_interface)

    async def parse(self, message):
        async with message.process():
            body = json.loads(message.body)

            logging.debug(f"Received message: {body}")

            data = dict(
                city=body.get("city", "n/a"),
                temperature_celsius=body.get(""),
                description=body.get("description", ""),
                source_provider=body.get('source_provider', "")
            )

            logging.debug(f"Publishing {data} to to_ship queue")
            self._rmq_interface.publish(settings.to_ship, data)

    async def run(self):
        await self._rmq_interface.connect()
        await self._rmq_interface.consume(self.parse, "fetched_weather")
