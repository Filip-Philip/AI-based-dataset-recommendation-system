from abc import ABC,abstractmethod
import pickle
# from dataclasses.DataCluster import DataCluster
# from dataclasses.EmbeddingBase import EmbeddingBase
from pandas import DataFrame
from typing import Dict, Set
import numpy as np

STD_SIZE = "std_size"
MEAN_SIZE = "mean_size"
COUNT = "count"
SIZES = "sizes"
PATHS = "paths"


def update_files_data(files_data: Dict, filetypes: Set) -> Dict:
    for filetype in filetypes:
        sizes = files_data.pop(f"{filetype}_{SIZES}")
        mean_size = sum(sizes) / files_data[f"{filetype}_{COUNT}"]
        files_data[f"{filetype}_{MEAN_SIZE}"] = mean_size
        std_size = np.sqrt(sum((np.array(sizes) - np.ones(len(sizes)) * mean_size) ** 2) / len(sizes))
        files_data[f"{filetype}_{STD_SIZE}"] = std_size

    return files_data


def save_json(repository: str, json_object: str, from_date: str, to_date: str):
    with open(f"../../../data/{repository}/backup_jsons/{from_date}-{to_date}.json", "w") as outfile:
        outfile.write(json_object)


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
            print("DEBUG {} LOG: {}".format(self.__class__,message))
         
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
