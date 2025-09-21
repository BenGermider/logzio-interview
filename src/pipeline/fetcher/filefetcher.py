from src.pipeline.fetcher.basic_fetcher import BasicFetcher


class FileFetcher(BasicFetcher):

    def __init__(self, rmq_interface, refresh_config_interval, file_path):
        super().__init__(rmq_interface, refresh_config_interval)
        self._file_path = file_path

    async def fetch(self):
        ...
