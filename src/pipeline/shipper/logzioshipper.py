import asyncio
import json
from typing import List, Dict

import httpx

from src.settings import settings
from src.pipeline.shipper.basicshipper import Shipper


class LOGZIOShipper(Shipper):
    def __init__(self, config, file_path: str):
        super().__init__(dest="https://{host}:8071/?token=<token>")
        self._file_path = file_path
        self.config = config


    async def batch(self, message):
        # Write each message as one line (append mode)
        async with message.process():
            with open(self._file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(message) + "\n")


    async def ship(self):
        while True:
            try:
                loaded_data = await self.load_messages()

                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        self._dest.format(host=settings.LOGZIO_URL, token=settings.LOGZIO_TOKEN),
                        headers={
                            "Content-Type": "application/json",
                        },
                        content='\n'.join([json.dumps(data) for data in loaded_data])
                    )
                    response.raise_for_status()
                await asyncio.sleep(60)

            except httpx.RequestError as e:
                # Network error, connection issues, timeout, etc.
                print(f"[Network error] Failed to send logs: {e}")
                await asyncio.sleep(10)  # retry sooner

            except httpx.HTTPStatusError as e:
                # Server returned 4xx/5xx
                print(f"[HTTP error] Status {e.response.status_code}: {e.response.text}")
                await asyncio.sleep(60)  # wait before retry

            except Exception as e:
                # Catch-all for unexpected errors
                print(f"[Unexpected error] {e}")
                await asyncio.sleep(60)



    async def load_messages(self) -> List[Dict]:
        """
        Loads newline-delimited JSON messages from a file.

        Returns:
            A list of dictionaries, each representing a stored message.
        """
        messages = []
        with open(self._file_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    data = json.loads(line)
                    messages.append(data)
        return messages

    async def run(self):
        asyncio.create_task(self.ship)
        await self.connect()
        await self._rmq_interface.consume(self.batch, settings.to_ship)