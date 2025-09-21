import csv
import asyncio
from src.pipeline.fetcher.filefetcher import FileFetcher
from src.pipeline.infra.rabbitmq.rabbitmq_interface import RabbitMQInterface


class CSVFetcher(FileFetcher):

    def __init__(self,refresh_config_interval, file_path, rmq_interface=RabbitMQInterface()):
        super().__init__(rmq_interface, refresh_config_interval, file_path)

    async def fetch(self):
        output = []
        with open(self._file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for city in self._cities:
                for row in reader:
                    if row.get("city") == city:
                        row['source_provider'] = self._file_path
                        output.append(row)

        return output

    async def run(self) -> None:
        """
        Runs the fetcher: connects to RabbitMQ, starts config reader, and periodically fetches and publishes data.
        :return:
        """
        await self.connect()
        asyncio.create_task(self.read_and_update_conf(2))
        while True:
            await self.publish()
            await asyncio.sleep(10)

