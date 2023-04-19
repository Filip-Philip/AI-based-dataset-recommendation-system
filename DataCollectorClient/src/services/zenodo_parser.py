import datetime

import requests
import json
import pandas as pd
import pickle
from typing import Dict
from datetime import datetime
from ParserBase import ParserBase
from dotenv import load_dotenv
import os
from collections import Counter

load_dotenv("environment.env")
ACCESS_TOKEN = os.getenv("ZENODO_KEY")


def unpack_metadata(dataset_list):
    for dataset in dataset_list:
        metadata = dataset.pop("metadata")
        dataset |= metadata
    return dataset_list


def gather_filetypes(dataset_list):
    for dataset in dataset_list:
        filetypes_list = [file["type"] for file in dataset["files"]]
        filetypes_dict = Counter(filetypes_list)
        dataset["filetypes"] = filetypes_dict
    return dataset_list


class ZenodoParser(ParserBase):
    ORIGINAL_COLUMN_NAMES = ["doi", "download_time", "title", "creators", "description", "keywords", "filetypes"]

    def __init__(self):
        self.data = pd.DataFrame()
        self.last_updated = None

    def download(self, number_of_datasets: int) -> None:
        response = requests.get('https://zenodo.org/api/records',
                                params={'access_token': ACCESS_TOKEN, 'size': number_of_datasets, 'type': 'dataset'})
        response_json = response.json()

        self.last_updated = datetime.now()

        datasets = response_json["hits"]["hits"]
        datasets = unpack_metadata(datasets)
        datasets = gather_filetypes(datasets)

        time_column_name = self.ORIGINAL_COLUMN_NAMES[1]
        for dataset in datasets:
            dataset[time_column_name] = self.last_updated

        self.data.append(pd.DataFrame.from_records(datasets))

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
            self.download(20000)

    def close(self):
        pass
