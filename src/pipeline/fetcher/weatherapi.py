import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, List
import sys
import httpx
import asyncio

CURRENT_DIR = Path(__file__).parent
CONFIG_FILE = CURRENT_DIR.parent / "config.json"

from src.pipeline.fetcher.basic_fetcher import BasicFetcher

# Basic configuration
logging.basicConfig(
    level=logging.INFO,  # or DEBUG if you want more details
    stream=sys.stdout,
    format="%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Get a logger for the current module
logger = logging.getLogger(__name__)


class OpenWeatherAPI(BasicFetcher):

    def __init__(self, rmq_interface, refresh_config_interval, api_key, source_url):
        super().__init__(rmq_interface, refresh_config_interval, api_key)
        self._source_url = source_url

    async def fetch(self) -> Optional[List[Dict[str, Any]]]:
        if not self._refresh_config_interval:
            return

        output = []
        for i in range(1, 6):
            try:
                async with httpx.AsyncClient() as client:
                    for city in self._cities:
                        resp = await client.get(self._source_url, params={'key': self._api_key, 'city': city})
                        resp = resp.json()
                        resp['source_provider'] = 'weatherapi'
                        output.append(resp)

            except Exception as e:
                logging.error(f"Attempt {i}/5 failed: {e}\nRetrying...")
                await asyncio.sleep(2)

        return output

    async def run(self) -> None:
        """
        Runs the fetcher: connects to RabbitMQ, starts config reader, and periodically fetches and publishes data.
        :return:
        """
        await self.connect()
        asyncio.create_task(self.read_and_update_conf(1))
        while True:
            await self.publish()
            await asyncio.sleep(10)