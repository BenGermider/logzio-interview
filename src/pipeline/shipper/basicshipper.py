from src.pipeline.infra.rabbitmq.rabbitmq_interface import RabbitMQInterface
from abc import ABC, abstractmethod


class Shipper(ABC):

    def __init__(self, dest: str, rmq_interface = RabbitMQInterface()):
        self._dest = dest
        self._rmq_interface = rmq_interface

    async def consume(self, q_name: str):
        await self._rmq_interface.consume(callback=self.batch, q_name=q_name)

    @abstractmethod
    async def ship(self):
        ...

    async def connect(self):
        await self._rmq_interface.create_connection()

    @abstractmethod
    async def batch(self, message):
        ...

