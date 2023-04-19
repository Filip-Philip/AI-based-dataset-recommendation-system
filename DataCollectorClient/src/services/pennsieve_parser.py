import datetime

import requests
import json
import pandas as pd
import pickle
from typing import Dict
from datetime import datetime
from ParserBase import ParserBase


class PennsieveParser(ParserBase):
    ORIGINAL_COLUMN_NAMES = ["doi", "download_time", "name", "contributors", "description", "tags", "filetypes"]

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
        time_column_name = self.ORIGINAL_COLUMN_NAMES[1]
        for dataset in datasets:
            dataset[time_column_name] = self.last_updated

        self.data.append(pd.DataFrame.from_records(datasets))

    def filter_out(self):
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
