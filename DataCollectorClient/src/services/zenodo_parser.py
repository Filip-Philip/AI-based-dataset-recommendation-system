import datetime

import requests
import json
import pandas as pd
from typing import List
import pickle
from typing import Dict, Set, Union, Any, KeysView
# from datetime import datetime
# from ParserBase import ParserBase
from dotenv import load_dotenv
import os
from collections import Counter
from .ParserBase import ParserBase, COUNT, SIZES, PATHS, OTHER, update_files_data, save_json, to_sparse
import numpy as np

load_dotenv("environment.env")
ACCESS_TOKEN = os.getenv("ZENODO_KEY")
ZENODO_CREATED_YEAR = 2013
ZENODO_CREATED_MONTH = 5
STATUS_OK = 200


def unpack_metadata(dataset_list):
    for dataset in dataset_list:
        metadata = dataset.pop("metadata")
        dataset |= metadata
    return dataset_list


def handle_filetype(filetype: str, file: Dict[str, Any], files_data: Dict[str, Union[int, List[int], List[str]]]):
    count = files_data.get(f"{filetype}_{COUNT}", 0)
    sizes = files_data.get(f"{filetype}_{SIZES}", [])
    paths = files_data.get(f"{filetype}_{PATHS}", [])
    count += 1
    sizes.append(file.get("size", 0))
    paths.append(file.get("links", dict()).get("self", ""))
    files_data[f"{filetype}_{COUNT}"] = int(count)
    files_data[f"{filetype}_{SIZES}"] = sizes
    files_data[f"{filetype}_{PATHS}"] = paths
    return files_data


def get_file_data(datasets, important_filetypes: KeysView):
    for dataset in datasets:
        files = dataset.get("files", [])
        files_data = dict()
        filetypes = set()
        for file in files:
            filetype = file.get("type", "")
            if filetype in important_filetypes:
                filetypes.add(filetype)
                files_data = handle_filetype(filetype, file, files_data)
            else:
                filetypes.add(OTHER)
                files_data = handle_filetype(OTHER, file, files_data)

        files_data = update_files_data(files_data, filetypes)

        dataset |= files_data

    return datasets


class ZenodoParser(ParserBase):

    ORIGINAL_COLUMN_NAMES = ["doi", "download_time", "created", "updated", "version", "title",
                             "creators", "description", "keywords", "filetypes", "filepaths"]
    DOWNLOAD_PERIOD_WEEKS = 4
    MAX_DOWNLOAD_PER_QUERY = 10000

    def __init__(self):
        self.data = pd.DataFrame()
        self.last_updated = None
        self.occurrence_count = dict()
        self.occurrence_ratio = dict()
        self.important_filetypes = dict()

    def download(self, number_of_datasets: int = MAX_DOWNLOAD_PER_QUERY, start_date: str = None,
                 end_date: str = None, preprocess: bool = False) -> None:
        if start_date is None and end_date is None:
            response = requests.get('https://zenodo.org/api/records', params={'access_token': ACCESS_TOKEN,
                                                                              'size': number_of_datasets,
                                                                              'type': 'dataset'})
        else:
            response = requests.get('https://zenodo.org/api/records',
                                    params={'access_token': ACCESS_TOKEN,
                                            'q': 'publication_date:{%(start)s TO %(end)s]' %
                                                 {'start': start_date, 'end': end_date},
                                            'size': number_of_datasets,
                                            'type': 'dataset'})

        if response.status_code == STATUS_OK:

            response_json = response.json()

            json_object = json.dumps(response_json)
            save_json("Zenodo", json_object, start_date, end_date)

            self.last_updated = datetime.datetime.now()
            datasets = response_json["hits"]["hits"]
            if preprocess:
                datasets = self.__gather_filetypes_and_paths(datasets)
            else:
                datasets = unpack_metadata(datasets)
                datasets = get_file_data(datasets, self.important_filetypes.keys())

            time_column_name = self.ORIGINAL_COLUMN_NAMES[1]
            for dataset in datasets:
                dataset[time_column_name] = self.last_updated

            this_period_df = pd.DataFrame.from_records(datasets)
            if preprocess:
                self.data = pd.concat([self.data, this_period_df])
            else:
                sparse_this_period_df = this_period_df.apply(to_sparse)
                self.data = pd.concat([self.data, sparse_this_period_df])

    def download_all_from_backup(self, preprocess: bool):
        for filename in os.scandir("../../../data/Zenodo/backup_jsons"):
            with open("../../../data/Zenodo/backup_jsons/" + filename.name, "r") as file:
                response_json = json.loads(file.read())

                self.last_updated = datetime.datetime.now()
                datasets = response_json["hits"]["hits"]
                if preprocess:
                    datasets = self.__gather_filetypes_and_paths(datasets)
                else:
                    datasets = unpack_metadata(datasets)
                    datasets = get_file_data(datasets, self.important_filetypes.keys())

                time_column_name = self.ORIGINAL_COLUMN_NAMES[1]
                for dataset in datasets:
                    dataset[time_column_name] = self.last_updated

                this_period_df = pd.DataFrame.from_records(datasets)
                if preprocess:
                    self.data = pd.concat([self.data, this_period_df])
                else:
                    sparse_this_period_df = this_period_df.apply(to_sparse)
                    self.data = pd.concat([self.data, sparse_this_period_df])
            break

    def download_all(self, preprocess: bool) -> None:
        start_date = datetime.datetime(2021, 8, 11).date()
        # start_date = datetime.datetime(ZENODO_CREATED_YEAR, ZENODO_CREATED_MONTH, 1).date()
        end_date = start_date + datetime.timedelta(weeks=+self.DOWNLOAD_PERIOD_WEEKS)
        while start_date < datetime.datetime.now().date():
            start_date_string = start_date.strftime('%Y-%m-%d')
            end_date_string = end_date.strftime('%Y-%m-%d')
            self.download(start_date=start_date_string, end_date=end_date_string, preprocess=preprocess)
            start_date += datetime.timedelta(weeks=+self.DOWNLOAD_PERIOD_WEEKS)
            end_date += datetime.timedelta(weeks=+self.DOWNLOAD_PERIOD_WEEKS)

    def get_target_data(self, top_n: int):
        self.get_important_filetypes(top_n)
        self.data = pd.DataFrame()
        self.download_all_from_backup(preprocess=False)

    def filter_out(self):
        return self.data[self.ORIGINAL_COLUMN_NAMES]

    def convert(self) -> Dict:
        return dict(zip(self.ORIGINAL_COLUMN_NAMES, self.BASE_COLUMN_NAMES))

    def create_embedding(self):
        pass

    def should_update(self, *args, **kwargs) -> bool:
        return False

    def update(self, older_than: datetime) -> None:
        if older_than > self.last_updated:
            self.download()

    def close(self):
        pass

    def __gather_filetypes_and_paths(self, dataset_list: List):
        for dataset in dataset_list:
            filetypes_list = [file["type"] for file in dataset.get("files", [])]
            filepaths = [file["links"]["self"] for file in dataset.get("files", [])]
            filetypes_dict = Counter(filetypes_list)
            dataset[self.ORIGINAL_COLUMN_NAMES[-2]] = filetypes_dict
            dataset[self.ORIGINAL_COLUMN_NAMES[-1]] = filepaths
        return dataset_list

    def count_occurrences(self, filetype_dict: Dict):
        sum_of_all = sum(filetype_dict.values())

        for key in filetype_dict:
            count = self.occurrence_count.get(key, 0)
            sum_of_all += filetype_dict[key]
            count += filetype_dict[key]
            self.occurrence_count[key] = count
            ratio = self.occurrence_ratio.get(key, 0)
            ratio += filetype_dict[key] / sum_of_all
            self.occurrence_ratio[key] = ratio

    def get_top_n_occurring_filetypes(self, n: int) -> Dict[str, int]:
        self.data[self.ORIGINAL_COLUMN_NAMES[-2]].apply(self.count_occurrences)
        top_n_filetypes = dict(sorted(list(self.occurrence_count.items()), key=lambda x: x[1], reverse=True)[:n])
        return top_n_filetypes

    def get_file_occurrence_captured(self, n):
        total_occurrence_count = sum(self.occurrence_count.values())
        top_n_filetypes = self.get_top_n_occurring_filetypes(n)
        print(top_n_filetypes)
        top_n_occurrence_count = sum(top_n_filetypes.values())
        top_n_occurrence_captured = top_n_occurrence_count / total_occurrence_count
        return top_n_occurrence_captured

    def get_important_filetypes(self, top_n: int):
        self.download_all_from_backup(preprocess=True)
        top_filetypes = self.get_top_n_occurring_filetypes(top_n)
        self.important_filetypes = top_filetypes


if __name__ == "__main__":
    parser = ZenodoParser()
    # parser.download()
    # parser = parser.load("pickle_test_all2.pickle")
    # parser.get_target_data(160)
    parser.download_all_from_backup(True)
    print(parser.data.loc[0])
    # parser.save("pickle_test_all_with_files_sparse.pickle")
    # dat = parser.load("pickle_test_all2.pickle").data

    # dat.astype(dtype)
    # print(dat.loc[0]["filetypes"])


