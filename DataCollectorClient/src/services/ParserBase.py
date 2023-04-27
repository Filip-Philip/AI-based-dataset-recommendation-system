from abc import ABC,abstractmethod
import pickle
# from dataclasses.DataCluster import DataCluster
# from dataclasses.EmbeddingBase import EmbeddingBase
from pandas import DataFrame


class ParserBase(ABC):
    BASE_COLUMN_NAMES = ["doi", "download_time", "created", "updated", "version", "title",
                         "authors", "description", "tags", "filetypes", "filepaths"]

    @abstractmethod
    def __init__(self):
        raise NotImplementedError

    def load(self, path):
        with open(path, 'rb') as file:
            return pickle.load(file)
    
    def debug_log(self,debug, message):
        if debug:
            print("DEBUG LOG:",message)
     
    
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
    def create_embedding(self, *args, **kwargs):
        raise NotImplementedError

    def to_pickle(self,  *args, **kwargs) -> bytes:
        return pickle.dumps(self)

    def save(self, filename: str) -> None:
        with open(filename, 'wb') as file:
            pickle.dump(self, file, protocol=pickle.HIGHEST_PROTOCOL)
    
    def to_dataframe(self, data:dict) -> DataFrame:
        return DataFrame(data)
