from datetime import datetime
import requests
from ParserBase import ParserBase
import pandas as pd


# convert code from scraping_dataverse.ipynb to class extending ParserBase.py
class DataverseParser(ParserBase):
    # https://guides.dataverse.org/en/5.13/api/search.html
    def __init__(self, url="https://dataverse.harvard.edu/api/search"):
        self.url = url
        self.data = None
        self.data_dict = None
        pass 
    
    BASE_COLUMN_NAMES = ["doi", "download_time", "created", "updated", "version", "title",
                        "authors", "description", "tags", "filetypes", "filepaths"]
    ORIGINAL_COLUMN_NAMES = ["global_id","download_time", "createdAt", "updatedAt", "versionId", "name", 
                             "authors", "publications", "keywords", "filetypes", "filepaths"]
    
    """
    t ['name', 'type', 'url', 'global_id', 'description', 'published_at',
       'publisher', 'citationHtml', 'identifier_of_dataverse',
       'name_of_dataverse', 'citation', 'storageIdentifier', 'subjects',
       'fileCount', 'versionId', 'versionState', 'majorVersion',
       'minorVersion', 'createdAt', 'updatedAt', 'contacts', 'authors',
       'download_time', 'filetypes', 'filepaths', 'producers', 'publications',
       'keywords', 'geographicCoverage', 'dataSources', 'relatedMaterial']
    """
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
        data_tmp = data.copy()
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
    dp = dp.load("dataverse")
    #generate brief report about this dataframe
    print(dp.data.info())
    print(dp.data.head())
    print(dp.data.describe())
    print(dp.data.columns)
    print(dp.data.dtypes)
    print(dp.data.isnull().sum())
    print(dp.data.isnull().sum().sum())
    print(dp.data.shape)
    print(dp.data.memory_usage())
    print(dp.data.memory_usage().sum())
    print(dp.data.memory_usage().sum()/1024**2)
    print(dp.data.memory_usage().sum()/1024**3)
    print(dp.data.memory_usage().sum()/1024**4)
    #for each collumn print first 2 non-null values so we can determine type of data
    for col in dp.data.columns:
        print(col,'\n',dp.data[col].dropna().head(2),'\n\n')  
        
    dp_converted =dp.convert(dp.data)
    
    for col in dp.data.columns:
        print(col,'\n',dp.data[col].dropna().head(2),'\n\n')
    
    