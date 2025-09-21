import json
import logging
import sys
from abc import ABC, abstractmethod
from pathlib import Path

import asyncio

logging.basicConfig(
    level=logging.INFO,  # or DEBUG if you want more details
    stream=sys.stdout,
    format="%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)
CURRENT_DIR = Path(__file__).parent
CONFIG_FILE = CURRENT_DIR.parent / "config.json"

class BasicFetcher(ABC):

    def __init__(self, rmq_interface, refresh_config_interval, api_key=None):
        self._rmq_interface = rmq_interface
        self._refresh_config_interval = refresh_config_interval
        self._api_key = api_key
        self._cities = set()

    @abstractmethod
    async def fetch(self):
        ...

    async def read_and_update_conf(self, index):
        while True:
            try:
                if CONFIG_FILE.exists():
                    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                        config_list = json.load(f)

                    if 0 <= index < len(config_list):
                        source = config_list[index]

                        new_cities = set(source.get("cities", []))
                        # Update only if different
                        if new_cities != set(self._cities):
                            self._cities = new_cities
                            print(f"[Config updated] Source {index} cities: {self._cities}")
                    else:
                        print(f"[Config read warning] source_index {index} out of range")

            except Exception as e:
                print(f"[Config read error] {e}")

            await asyncio.sleep(self._refresh_config_interval)

    async def publish(self):
        data = await self.fetch()
        if not data:
            logger.warning("No data fetched to publish")
            return

        await self._rmq_interface.publish(data, "fetched_weather")

    async def connect(self):
        await self._rmq_interface.create_connection()

    @abstractmethod
    async def run(self):
        ...