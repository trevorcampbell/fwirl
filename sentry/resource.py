from abc import abstractmethod

class Resource:
    def __init__(self, key):
        self.hash = hash(key)
        self.key = key

    def __hash__(self):
        return self.hash

    def __eq__(self, rhs):
        return self.hash == rhs.hash

    def __repr__(self):
        return self.__class__.__name__ + f"({self.key})"
 
    @abstractmethod
    def open(self):
        pass

    @abstractmethod
    def close(self):
        pass
