import datetime

import requests
import json
import pandas as pd
import pickle
from typing import Dict
from datetime import datetime
from ParserBase import ParserBase
import numpy as np
from ParserBase import COUNT, SIZES, PATHS, update_files_data


def get_file_data(datasets):
    for dataset in datasets:
        url = f"https://api.pennsieve.io/discover/datasets/{dataset['id']}/versions/{dataset['version']}/metadata"

        headers = {"accept": "application/json"}

        response = requests.get(url, headers=headers)
        if type(response.json()) == dict:
            files = response.json()["files"]
            files_data = dict()
            filetypes = set()
            for file in files:
                filetype = file.get("fileType", "")
                filetypes.add(filetype)
                count = files_data.get(f"{filetype}_{COUNT}", 0)
                sizes = files_data.get(f"{filetype}_{SIZES}", [])
                paths = files_data.get(f"{filetype}_{PATHS}", [])
                count += 1
                sizes.append(file.get("size", 0))
                paths.append(file.get("path", ""))
                files_data[f"{filetype}_{COUNT}"] = int(count)
                files_data[f"{filetype}_{SIZES}"] = sizes
                files_data[f"{filetype}_{PATHS}"] = paths

            files_data = update_files_data(files_data, filetypes)

            dataset |= files_data

    return datasets


class PennsieveParser(ParserBase):

    ORIGINAL_COLUMN_NAMES = ["doi", "download_time", "createdAt", "updatedAt", "version", "name",
                             "contributors", "description", "tags", "filetypes", "filepaths"]

    def __init__(self):
        self.data = pd.DataFrame()
        self.last_updated = None

    def download(self, number_of_datasets: int) -> None:
        url = f"https://api.pennsieve.io/discover/datasets?limit={number_of_datasets}" \
              f"&offset=0&orderBy=relevance&orderDirection=desc"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        response_json = json.loads(response.text)

        self.last_updated = datetime.now()

        datasets = response_json["datasets"]
        datasets = get_file_data(datasets)

        time_column_name = self.ORIGINAL_COLUMN_NAMES[1]
        for dataset in datasets:
            dataset[time_column_name] = self.last_updated

        self.data = pd.concat([self.data, pd.DataFrame.from_records(datasets)])

    def filter_out(self) -> pd.DataFrame:
        return self.data[self.ORIGINAL_COLUMN_NAMES]

    def convert(self) -> Dict:  # {pennsieve_df_column_name -> general_df_column_name}
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


if __name__ == "__main__":
    parser = PennsieveParser()
    # parser.download(100000)
    # parser.save("pickle_test_pennsieve_mean_std.pickle")
    print(parser.load("pickle_test_pennsieve_mean_std.pickle").data.loc[0].values)
