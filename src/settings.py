from pydantic import BaseSettings

class Settings(BaseSettings):

    REFRESH_CONFIG_INTERVAL: int = 60  # seconds

    # RabbitMQ Settings
    RABBITMQ_HOST: str = "rabbitmq"
    RABBITMQ_USER: str = "logzio"
    RABBITMQ_PASS: str = "logzio"

    # API Keys
    OPEN_WEATHER_MAP_API_KEY: str = ""
    WEATHERAPI_API_KEY: str = ""

    # Shipping
    LOGZIO_TOKEN: str = ""
    LOGZIO_URL: str = "https://listener.logz.io:8071"

    # Rabbitmq Queues
    fetched_weather: str = "fetched_weather"
    to_ship: str = "to_ship"

    class Config:
        env_file = ".env"


settings = Settings()
