from abc import ABC,abstractmethod
import pickle
from dataclasses.DataCluster import DataCluster
from dataclasses.EmbeddingBase import EmbeddingBase
from pandas import DataFrame


class ParserBase(ABC):
    BASE_COLUMN_NAMES = ["doi", "download_time", "title", "authors", "description", "tags", "filetypes"]

    @abstractmethod
    def __init__(self):
        raise NotImplementedError

    @abstractmethod 
    def load(self,path):
        pass
     
    @abstractmethod
    def download(self, *args, **kwargs): 
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
    
    def to_dataframe(self, data:dict) -> DataFrame:
        return DataFrame(data)