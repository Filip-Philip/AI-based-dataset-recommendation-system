from datetime import datetime
import requests
from tqdm import tqdm
from ParserBase import ParserBase
import pandas as pd
import aiohttp
import asyncio
from tqdm.asyncio import tqdm_asyncio
import time
import sys
import os
import json

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
# convert code from scraping_dataverse.ipynb to class extending ParserBase.py
dataverse_key = "7011421a-9032-4c3a-b8bb-242f83c67c66"
async def get_dataverse_info(dois, api_key):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for doi in dois:
                url = f'https://dataverse.harvard.edu/api/datasets/:persistentId/?persistentId={doi}&key={api_key}'
                print(url)
                tasks.append(asyncio.ensure_future(get_data(session, url)))
            responses = await tqdm_asyncio.gather(*tasks)
            await asyncio.sleep(0.5) 
            return responses
async def get_data(session, url):
    try:
        async with session.get(url) as response:
            data = await response.json()
            return data
    except aiohttp.ClientError as e:
        print(e)
        return None

def get_dataverse_files(dois, api_key):
    responses = asyncio.get_event_loop().run_until_complete(get_dataverse_info(dois, api_key))

    return responses

class DataverseParser(ParserBase):
    # https://guides.dataverse.org/en/5.13/api/search.html
    base_dir = """C:/Users/Luki/Documents/Studia/Inzynierka/AI-based-dataset-recommendation-system/data/Dataverse/"""
    def __init__(self, url="https://dataverse.harvard.edu/api/search"):
        self.url = url
        self.data = None
        self.data_dict = None
    
    BASE_COLUMN_NAMES = ["doi", "download_time", "created", "updated", "version", "title",
                        "authors", "description", "tags", "filetypes", "filepaths"]
    ORIGINAL_COLUMN_NAMES = ["global_id","download_time", "createdAt", "updatedAt", "versionId", "name", 
                             "authors", "publications", "keywords", "filetypes", "filepaths"]
    
    """
    [
       'name', 'type', 'url', 'global_id', 'description', 'published_at',
       'publisher', 'citationHtml', 'identifier_of_dataverse',
       'name_of_dataverse', 'citation', 'storageIdentifier', 'subjects',
       'fileCount', 'versionId', 'versionState', 'majorVersion',
       'minorVersion', 'createdAt', 'updatedAt', 'contacts', 'authors',
       'download_time', 'filetypes', 'filepaths', 'producers', 'publications',
       'keywords', 'geographicCoverage', 'dataSources', 'relatedMaterial'
    ]
    """
    def load_file_information(self):
        #load file information from json files and add them to data_dict
        lodaded =0
        errored =0
        for data in tqdm_asyncio(self.data_dict):
            doi_path = self.base_dir + "files_info/" + data["persistentId"].replace("/", "_").replace(":","-") + ".json"
            if not os.path.exists(doi_path):
                errored +=1
                continue
            with open(doi_path, "r") as f:
                response = json.load(f)
                if response is None:
                    errored +=1
                    continue
                data["filetypes"] = []
                data["filepaths"] = []
                for file in response["data"]["latestVersion"]["files"]:
                    data["filetypes"].append(file["dataFile"]["contentType"])
                    data["filepaths"].append(file["dataFile"]["storageIdentifier"])
                lodaded +=1
        print("Loaded: " + str(lodaded) + " errored: " + str(errored))
                    
    def download_file_information(self, only_missing:bool = True):
        #load excluded dois
        excluded_dois = set()
        if os.path.exists(self.base_dir + "excluded_dois.txt"):
            with open(self.base_dir + "excluded_dois.txt", "r") as f:
                for i in f.read().splitlines():
                    excluded_dois.add(i)
         
        dois = self.data["doi"].tolist()
        if only_missing:
            dois = [doi for doi in dois if (not os.path.exists(self.base_dir + "files_info/" + doi.replace("/", "_").replace(":", "-") + ".json") and doi not in excluded_dois)]
        chunk_size = 1000
        chunks = [dois[i:i + chunk_size] for i in range(0, len(dois), chunk_size)]
        chunk_num = 0
        for chunk in tqdm_asyncio(chunks):
            try:
                responses = get_dataverse_files(chunk, dataverse_key)

            except Exception as e:
                print(e)
            #SAVE RESPONSES TO JSON FILES
            for response in responses:
                if response is None:
                    continue
                try:
                    identification_code = response["data"]["protocol"] + ":" + response["data"]["authority"] + "/" + response["data"]["identifier"]
                    f_path = self.base_dir + "files_info/" +identification_code.replace(":","-").replace("/","_")+ ".json"
                    with open(f_path, "w") as f:
                        json.dump(response, f)          
                except OSError as e:
                    #print whole exception information
                    print(e)
                    break
                except KeyError as e:
                    print(e)
                    print(response)
                    continue 
    def download(self, query="Neuroscience", start=1, per_page=1000, debug=False):
        dataverse_metadata = []
        start = 1
        headers = {"X-Dataverse-key": "b7d3b5a0-5b5a-4b9f-8f9f-3c6b6b6b6b6b"}
        while True:
            url : str = self.url +"?q=*&type=dataset&per_page={}&start={}".format(str(per_page),str(start))
            self.debug_log(debug, "Downloading page: " + str(start) + " url: " + url)
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                print("Error: ", response.status_code)
                print(response.text)
                return None
            
            response_json = response.json()
            if len(response_json['data']["items"]) == 0:
                break
            dataverse_metadata.extend(response_json['data']["items"])
            
            start += per_page
        for data in dataverse_metadata:
            data["download_time"] = datetime.now()
            data["filetypes"] = []
            data["filepaths"] = []
        
        self.data_dict = dataverse_metadata
        self.data = self.to_dataframe(dataverse_metadata) 
    
    def filter_out(self,data:pd.DataFrame, in_place=False):
        return self.data[self.ORIGINAL_COLUMN_NAMES]
    
    def correct_types(data:pd.DataFrame, in_place=False):
        #convert columns to correct types
        data_tmp = data.copy(True)
        data_tmp["download_time"] = pd.to_datetime(data["download_time"])
        data_tmp["createdAt"] = pd.to_datetime(data["createdAt"])
        data_tmp["updatedAt"] = pd.to_datetime(data["updatedAt"])
        data_tmp["versionId"] = pd.to_numeric(data["versionId"])
        data_tmp["fileCount"] = pd.to_numeric(data["fileCount"])
        data_tmp["majorVersion"] = pd.to_numeric(data["majorVersion"])
        data_tmp["minorVersion"] = pd.to_numeric(data["minorVersion"])
        data_tmp["doi"] = data["doi"].astype(str)
        data_tmp["title"] = data["title"].astype(str)
        #convert authors to list of strings
        return data_tmp  
         
    def convert(self, data:pd.DataFrame, in_place=False):
        filtered = self.filter_out(data)
        column_name_map = dict(zip(self.ORIGINAL_COLUMN_NAMES,self.BASE_COLUMN_NAMES))
        filtered = filtered.rename(columns=column_name_map)
        return filtered 
    
    def create_embedding(self):
        pass
    
    def should_update(self, *args, **kwargs) -> bool:
        return False
    
    
    
    def update(self, *args, **kwargs):
        pass
    
    

if __name__ == "__main__":
    dp = DataverseParser()
    #dp.download(debug=True)
    #dp.save( "dataverse")
    dp = dp.load(dp.base_dir+"dataverse")

    #generate brief report about this dataframe
    
    print(dp.base_dir)
    
    """
    print(dp.data.info())
    print(dp.data.head())
    print(dp.data.describe())
    print(dp.data.columns)
    print(dp.data.dtypes)
    print(dp.data.isnull().sum())
    print(dp.data.shape)
    print(dp.data.memory_usage())
    print(dp.data.memory_usage().sum())
    print(dp.data.memory_usage().sum()/1024)
    #for each collumn print first 2 non-null values so we can determine type of data
    for col in dp.data.columns:
        print(col,'\n',dp.data[col].dropna().head(2),'\n\n')  
    """
    
    dp.data =dp.convert(dp.data)
    
    """dp_withfiles = dp.get_filetype_information(dp_converted, "doi")
    dp.data = dp_withfiles
    dp.save( "dataverse_withfiles")
    print(dp_withfiles["files"].head())
    """
    
    dp.download_file_information()

    
    