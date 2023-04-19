from datetime import datetime
import requests
from ParserBase import ParserBase
from dataclasses.DataCluster import DataCluster
import pandas as pd

#convert code from scraping_dryad.ipynb to class extending ParserBase.py
class DryadParser(ParserBase):
    def __init__(self, url = "https://datadryad.org/api/v2/search"):
        self.url = url
        self.data = None
        self.data_dict = None
        pass 
    
    def download(self, query="Neuroscience", page=1, per_page=100):
        dryad_metadata = []
        page = 1
        while True:
            url : str = self.url + str(page)
            headers = {'Content-Type': 'application/json'}
            response = requests.get(url, headers=headers)
            response_json = response.json()
            if len(response_json['_embedded']["stash:datasets"]) == 0:
                break
            dryad_metadata.extend(response_json['_embedded']["stash:datasets"])
            page += 1
        self.data_dict = dryad_metadata
        self.data = self.to_dataframe(dryad_metadata) 
    
    def filter_out(self,data:pd.DataFrame, in_place=False *args, **kwargs):
        return data.drop(['_links','versionNumber', 'versionChanges'], axis=1, inplace=in_place)

