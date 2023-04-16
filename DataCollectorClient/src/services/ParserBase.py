from abc import ABC,abstractmethod
import pickle
from dataclasses.DataCluster import DataCluster
from dataclasses.EmbeddingBase import EmbeddingBase
from pandas import DataFrame


class ParserBase(ABC):
    @abstractmethod
    def __init__(self):
        raise NotImplementedError
        
    @abstractmethod
    def download(self, *args, **kwargs)-> DataFrame: 
        raise NotImplementedError
    @abstractmethod 
    def filter_out(self, *args, **kwargs) -> DataFrame:
        raise NotImplementedError
      
    @abstractmethod
    def convert(self, *args, **kwargs) -> DataFrame:
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
    
     
    def to_pickle(self, *args, **kwargs) -> bytes:
        return pickle.dumps(self) 
    