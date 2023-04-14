import datetime

import requests
import json
import pandas as pd
import pickle
from typing import Dict
from datetime import datetime
from parser_base import ParserBase


class PennsieveParser(ParserBase):
    def __init__(self):
        self.data = None
        self.data_dict = None
        self.last_updated = None

    def download(self, number_of_datasets: int) -> None:
        url = f"https://api.pennsieve.io/discover/datasets?limit={number_of_datasets}" \
              f"&offset=0&orderBy=relevance&orderDirection=desc"

        headers = {"accept": "application/json"}

        response = requests.get(url, headers=headers)

        self.last_updated = datetime.now()

        response_json = json.loads(response.text)

        datasets = response_json["datasets"]

        id_list = [dataset['id'] for dataset in datasets]

        self.data_dict = dict(zip(id_list, datasets))

        self.data = pd.DataFrame.from_records(datasets)

    def filter(self):
        pass

    def to_pickle(self) -> bytes:
        return pickle.dumps(self)

    def convert(self) -> Dict: # {pennsieve_df_column_name -> general_df_column_name}
        return self.data_dict

    def create_embedding(self):
        pass

    def update(self, older_than: datetime) -> None:
        if older_than > self.last_updated:
            self.download(20000)

    def close(self):
        pass
