from abc import ABC, abstractmethod

from src.pipeline.infra.rabbitmq.rabbitmq_interface import RabbitMQInterface


class BasicParser(ABC):

    def __init__(self, rmq_interface=RabbitMQInterface()):
        self._rmq_interface = rmq_interface

    @abstractmethod
    async def parse(self, message):
        ...
