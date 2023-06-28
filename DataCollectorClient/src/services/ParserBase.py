import os.path
from abc import ABC, abstractmethod
from datetime import datetime
import pickle
# from dataclasses.DataCluster import DataCluster
# from dataclasses.EmbeddingBase import EmbeddingBase
from pandas import DataFrame
from typing import Dict, Set
import numpy as np
import pandas as pd

STD_SIZE = "std_size"
MEAN_SIZE = "mean_size"
COUNT = "count"
SIZES = "sizes"
PATHS = "paths"
OTHER = "other"
SPARSE_CONVERSION_CONSTANT = 0.3
DOWNLOAD_PERIOD_DAYS = 1


# def convert_to_filetype_mean_std(row, df):
#     size_dict = dict()
#     for file in row.get("files", []):
#         filetype = file.get("type", "")
#         size_list = size_dict.get(filetype, [])
#         size_list.append(file.get("size", 0))
#         size_dict[filetype] = size_list
#
#     for filetype, count in row.get("filetypes", dict()).items():
#         sizes = size_dict.pop(filetype)
#         mean_size = sum(sizes) / count
#         columns = [f"{filetype}_{MEAN_SIZE}", f"{filetype}_{STD_SIZE}"]
#         spdtypes = df.dtypes[columns]
#         df[columns] = df[columns].sparse.to_dense()
#         # row[f"{filetype}_{MEAN_SIZE}"] = mean_size
#         df.loc[row.name, f"{filetype}_{MEAN_SIZE}"] = mean_size
#         std_size = np.sqrt(sum((np.array(sizes) - np.ones(count) * mean_size) ** 2) / count)
#         df.loc[row.name, f"{filetype}_{STD_SIZE}"] = std_size
#         df[columns] = df[columns].astype(spdtypes)
#
#         # row[f"{filetype}_{STD_SIZE}"] = pd.arrays.SparseArray(std_size)
#
#     return df.loc[row.name]


def to_sparse(x):
    if x.isna().sum() / len(x) > SPARSE_CONVERSION_CONSTANT:
        return pd.arrays.SparseArray(x)
    return x


def update_files_data(files_data: Dict, filetypes: Set) -> Dict:
    for filetype in filetypes:
        sizes = files_data.pop(f"{filetype}_{SIZES}")
        mean_size = sum(sizes) / files_data[f"{filetype}_{COUNT}"]
        files_data[f"{filetype}_{MEAN_SIZE}"] = mean_size
        std_size = np.sqrt(sum((np.array(sizes) - np.ones(len(sizes)) * mean_size) ** 2) / len(sizes))
        files_data[f"{filetype}_{STD_SIZE}"] = std_size

    return files_data


def save_json(repository_data_path: str, json_object: str, from_date: str, to_date: str):
    with open(f"{repository_data_path}/backup_jsons/{from_date}-{to_date}.json", "w") as outfile:
        outfile.write(json_object)


def save_json_day(repository_data_path: str, json_object: str, year: int, month: int, day: int):
    filename = os.path.join(repository_data_path, 'backup_jsons', str(year), str(month), str(day) + '.json')
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as outfile:
        outfile.write(json_object)


class ParserBase(ABC):
    BASE_COLUMN_NAMES = ["doi", "download_time", "created", "updated", "version", "title",
                         "authors", "description", "tags", "filetypes", "filepaths"]
    base_dir =""
    @abstractmethod
    def __init__(self):
        raise NotImplementedError

    def load(self, path):
        with open(path, 'rb') as file:
            return pickle.load(file)

    def debug_log(self, debug, message):
        if debug:
            print("DEBUG {} LOG: {}".format(self.__class__, message))

    @abstractmethod
    def download(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def filter_out(self, in_place=False) -> pd.DataFrame | None:
        raise NotImplementedError

    @abstractmethod
    def convert(self, in_place=False) -> pd.DataFrame | None:
        raise NotImplementedError

    @abstractmethod
    def check_api_key(self):
        raise NotImplementedError

    @abstractmethod
    def update(self, *args, **kwargs):
        """Used as main function which will be used to download and update data from the source

        Raises:
            NotImplementedError: Abstract class does not have implementation. See child classes 
        """
        raise NotImplementedError

    @abstractmethod
    def should_update(self, *args, **kwargs) -> bool:
        raise NotImplementedError

    @abstractmethod
    def create_embedding(self, *args, **kwargs):
        raise NotImplementedError

    def save_title_description_json(self, data: DataFrame, filename: str):
        data[["title", "description", "doi"]].to_json(filename, orient="records")

    def to_pickle(self, *args, **kwargs) -> bytes:
        return pickle.dumps(self)

    def save(self, filename: str) -> None:
        with open(filename, 'wb') as file:
            pickle.dump(self, file, protocol=pickle.HIGHEST_PROTOCOL)

    def to_dataframe(self, data: dict) -> DataFrame:
        return DataFrame(data)

    """utility functions"""

    def clean_alt_list(list_: str):
        list_ = list_.replace(', ', '","')
        list_ = list_.replace('[', '["')
        list_ = list_.replace(']', '"]')
        return list_


import torch
import tqdm

# device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
#
#
# def make_data_embedding(title_description_metadata, tokenizer, model, method="mean", dim=1):
#     # keep track what embeddings are done and save that to file
#     embedding_list = []
#
#     for i in tqdm.tqdm(range(len(title_description_metadata))):
#
#         embedding = embed_text(title_description_metadata[i], tokenizer, model)
#
#         if method == "mean":
#             embedding_list.append(embedding.mean(dim).to(device))
#
#     return embedding_list
#
#
# description_embedding = make_data_embedding(title_description_metadata_untrashed["text"].values, tokenizer, model)
