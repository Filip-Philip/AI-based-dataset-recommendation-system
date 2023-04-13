from abc import ABC,abstractmethod
from dataclasses.DataCluster import DataCluster
from dataclasses.EmbeddingBase import EmbeddingBase


class ParserBase(ABC):
    @abstractmethod
    def __init__(self):
        raise NotImplementedError
        
    @abstractmethod
    def download(self, *args, **kwargs)-> DataCluster: 
        raise NotImplementedError
    
    @abstractmethod
    def update(self, *args, **kwargs):
        raise NotImplementedError
    
    @abstractmethod
    def should_update(self, *args, **kwargs) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    def create_embedding(self, *args, **kwargs) -> EmbeddingBase:
        raise NotImplementedError
    
    