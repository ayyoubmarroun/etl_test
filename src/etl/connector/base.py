from abc import ABC, abstractmethod, abstractproperty

class BaseConnector(ABC):
    
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def append_data(self):
        pass

    @abstractmethod
    def query(self):
        pass
    