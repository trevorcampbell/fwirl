from abc import abstractmethod

class GraphWorker:
    def __init__(self, watched_assets):
        self.watched_assets = watched_assets

    @abstractmethod
    async def restructure(self, graph):
        pass

