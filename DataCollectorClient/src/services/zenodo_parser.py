import datetime

import requests
import json
import pandas as pd
from typing import List
import pickle
from typing import Dict
# from datetime import datetime
from ParserBase import ParserBase
from dotenv import load_dotenv
import os
from collections import Counter

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


# def gather_filetypes_and_paths(dataset_list):
#     for dataset in dataset_list:
#         filetypes_list = [file["type"] for file in dataset.get("files", [])]
#         filepaths = [file["links"]["self"] for file in dataset.get("files", [])]
#         filetypes_dict = Counter(filetypes_list)
#         dataset["filetypes"] = filetypes_dict
#         dataset["filepaths"] = filepaths
#     return dataset_list


class ZenodoParser(ParserBase):

    ORIGINAL_COLUMN_NAMES = ["doi", "download_time", "created", "updated", "version", "title",
                             "creators", "description", "keywords", "filetypes", "filepaths"]
    DOWNLOAD_PERIOD_WEEKS = 4
    MAX_DOWNLOAD_PER_QUERY = 10000

    def __init__(self):
        self.data = pd.DataFrame()
        self.last_updated = None

    def download(self, number_of_datasets: int = MAX_DOWNLOAD_PER_QUERY, start_date: str = None,
                 end_date: str = None) -> None:
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
            self.last_updated = datetime.datetime.now()
            datasets = response_json["hits"]["hits"]
            datasets = unpack_metadata(datasets)
            datasets = self.__gather_filetypes_and_paths(datasets)

            time_column_name = self.ORIGINAL_COLUMN_NAMES[1]
            for dataset in datasets:
                dataset[time_column_name] = self.last_updated

            self.data = pd.concat([self.data, pd.DataFrame.from_records(datasets)])

    def download_all(self) -> None:
        start_date = datetime.datetime(ZENODO_CREATED_YEAR, ZENODO_CREATED_MONTH, 1).date()
        end_date = start_date + datetime.timedelta(weeks=+self.DOWNLOAD_PERIOD_WEEKS)
        while start_date < datetime.datetime.now().date():
            start_date_string = start_date.strftime('%Y-%m-%d')
            end_date_string = end_date.strftime('%Y-%m-%d')
            self.download(start_date=start_date_string, end_date=end_date_string)
            start_date += datetime.timedelta(weeks=+self.DOWNLOAD_PERIOD_WEEKS)
            end_date += datetime.timedelta(weeks=+self.DOWNLOAD_PERIOD_WEEKS)

    def filter_out(self):
        return self.data[self.ORIGINAL_COLUMN_NAMES]

    def convert(self) -> Dict:
        return dict(zip(self.ORIGINAL_COLUMN_NAMES, self.BASE_COLUMN_NAMES))

    def create_embedding(self):
        pass

    def should_update(self, *args, **kwargs) -> bool:
        return False

    def load(self, path):
        with open(path, 'rb') as file:
            return pickle.load(file)

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


if __name__ == "__main__":
    parser = ZenodoParser()
    parser.download_all()
    parser.save("pickle_test_all2.pickle")
    # print(len(parser.load("pickle_test_all.pickle").data))

