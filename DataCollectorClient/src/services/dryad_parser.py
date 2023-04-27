from datetime import datetime
import requests
from ParserBase import ParserBase
import pandas as pd


# convert code from scraping_dryad.ipynb to class extending ParserBase.py
class DryadParser(ParserBase):
    def __init__(self, url="https://datadryad.org/api/v2/search"):
        self.url = url
        self.data = None
        self.data_dict = None
        pass 
    
    BASE_COLUMN_NAMES = ["doi", "download_time", "created", "updated", "version", "title",
                        "authors", "description", "tags", "filetypes", "filepaths"]
    ORIGINAL_COLUMN_NAMES = ["identifier","download_time", "publicationDate", "lastModificationDate", "versionNumber", "title", 
                             "authors", "abstract", "keywords", "filetypes", "filepaths"]

    def download(self, query="Neuroscience", page=1, per_page=100, debug=False):
        dryad_metadata = []
        page = 1
        while True:
            url : str = self.url +"?per_page={}&page={}".format(str(per_page),str(page))
            headers = {'Content-Type': 'application/json'}
            self.debug_log(debug, "Downloading page: " + str(page) + " url: " + url)
            
            response = requests.get(url, headers=headers)
            
            if response.status_code != 200:
                print("Error: ", response.status_code)
                print(response.text)
                return None

            response_json = response.json()
            if len(response_json['_embedded']["stash:datasets"]) == 0:
                break
            dryad_metadata.extend(response_json['_embedded']["stash:datasets"])
            
            page += 1
        for data in dryad_metadata:
            data["download_time"] = datetime.now()
            data["filetypes"] = []
            data["filepaths"] = []
        
        self.data_dict = dryad_metadata
        self.data = self.to_dataframe(dryad_metadata) 
    
    def filter_out(self,data:pd.DataFrame, in_place=False):
        #return data.drop(['_links','versionNumber', 'versionChanges'], axis=1, inplace=in_place)
        return
    
    def convert(self, data:pd.DataFrame, in_place=False):
        #data = data.rename(columns={'publicationDate': 'publication_date', 'publicationYear': 'publication_year', 'publicationMonth': 'publication_month', 'publicationDay': 'publication_day', 'publicationDateType': 'publication_date_type', 'publicationDateAccuracy': 'publication_date_accuracy', 'publicationDateYearOnly': 'publication_date_year_only', 'publicationDateSeason': 'publication_date_season', 'publicationDateDecade': 'publication_date_decade', 'publicationDateCentury': 'publication_date_century', 'publicationDateDisplay': 'publication_date_display', 'publicationDateFreeform': 'publication_date_freeform', 'publicationDateSort': 'publication_date_sort', 'publicationDateNormalized': 'publication_date_normalized', 'publicationDateNormalizedYear': 'publication_date_normalized_year', 'publicationDateNormalizedMonth': 'publication_date_normalized_month', 'publicationDateNormalizedDay': 'publication_date_normalized_day'}, inplace=in_place)
        return dict(zip(self.ORIGINAL_COLUMN_NAMES,self.BASE_COLUMN_NAMES))
    
    def create_embedding(self):
        pass
    
    def should_update(self, *args, **kwargs) -> bool:
        return False
    
    def update(self, *args, **kwargs):
        pass
    
    

if __name__ == "__main__":
    dp = DryadParser()
    dp.download(debug=True)
    dp.save( "dryad")
    dp.data = dp.convert(dp.data)
    dp.save("dryad_converted") 
    
    print(dp.data)
    