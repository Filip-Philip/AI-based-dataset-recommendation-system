from typing import Dict, List

import numpy as np
import pandas as pd

from DataCollectorClient.src.services.ParserBase import OTHER, MEAN_SIZE, STD_SIZE


class UnifiedContainer:
    BASE_COLUMN_NAMES = ["doi", "download_time", "created", "updated", "version", "title",
                         "authors", "description", "tags", "filetypes", "filepaths"]

    def __init__(self):
        self.data = pd.DataFrame()
        self.occurrence_count = dict()
        self.occurrence_ratio = dict()
        self.important_filetypes = dict()

    def append_data(self, data: List[pd.DataFrame]):
        for df in data:
            self.data = pd.concat([self.data, df])

    def get_target_data(self, top_n: int):
        self.__get_important_filetypes(top_n)
        self.__to_filetype_mean_std_sparse_format(top_n)

    def __get_file_occurrence_captured(self, n):
        total_occurrence_count = sum(self.occurrence_count.values())
        top_n_filetypes = self.__get_top_n_occurring_filetypes(n)
        top_n_occurrence_count = sum(top_n_filetypes.values())
        top_n_occurrence_captured = top_n_occurrence_count / total_occurrence_count
        return top_n_occurrence_captured

    def __get_important_filetypes(self, top_n: int):
        top_filetypes = self.__get_top_n_occurring_filetypes(top_n)
        top_filetypes[OTHER] = 0
        self.important_filetypes = top_filetypes

    def __get_top_n_occurring_filetypes(self, n: int) -> Dict[str, int]:
        self.data[self.BASE_COLUMN_NAMES[-2]].apply(self.__count_occurrences)
        top_n_filetypes = dict(sorted(list(self.occurrence_count.items()), key=lambda x: x[1], reverse=True)[:n])
        return top_n_filetypes

    def __count_occurrences(self, filetype_dict: Dict):
        sum_of_all = sum(filetype_dict.values())

        for key in filetype_dict:
            count = self.occurrence_count.get(key, 0)
            sum_of_all += filetype_dict[key]
            count += filetype_dict[key]
            self.occurrence_count[key] = count
            ratio = self.occurrence_ratio.get(key, 0)
            ratio += filetype_dict[key] / sum_of_all
            self.occurrence_ratio[key] = ratio

    def __to_filetype_mean_std_sparse_format(self):
        for filetype in self.important_filetypes:
            self.data[f"{filetype}_{MEAN_SIZE}"] = np.nan
            self.data[f"{filetype}_{MEAN_SIZE}"] = pd.arrays.SparseArray(self.data[f"{filetype}_{MEAN_SIZE}"])
            self.data[f"{filetype}_{STD_SIZE}"] = np.nan
            self.data[f"{filetype}_{STD_SIZE}"] = pd.arrays.SparseArray(self.data[f"{filetype}_{STD_SIZE}"])

        self.data.progress_apply(self.__convert_to_filetype_mean_std, axis=1)

    def __convert_to_filetype_mean_std(self, row):
        size_dict = dict()
        try:
            for file in row.get("files", []):
                filetype = file.get("type", "")
                filetype = filetype if filetype in self.important_filetypes.keys() else OTHER
                size_list = size_dict.get(filetype, [])
                size_list.append(file.get("size", 0))
                size_dict[filetype] = size_list
        except TypeError as e:
            pass

        for filetype, sizes in size_dict.items():
            count = len(sizes)
            mean_size = sum(sizes) / count
            columns = [f"{filetype}_{MEAN_SIZE}", f"{filetype}_{STD_SIZE}"]
            spdtypes = self.data.dtypes[columns]
            self.data[columns] = self.data[columns].sparse.to_dense()
            self.data.loc[row.name, f"{filetype}_{MEAN_SIZE}"] = mean_size
            std_size = np.sqrt(sum((np.array(sizes) - np.ones(count) * mean_size) ** 2) / count)
            self.data.loc[row.name, f"{filetype}_{STD_SIZE}"] = std_size
            self.data[columns] = self.data[columns].astype(spdtypes)
