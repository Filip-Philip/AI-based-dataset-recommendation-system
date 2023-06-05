from ParserBase import ParserBase
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
import aiohttp
import asyncio
import json
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import pathlib
import regex as re
import requests
import sys

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
# convert code from scraping_dataverse.ipynb to class extending ParserBase.py
dataverse_key = os.environ.get('DATAVERSE_API_KEY')

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

def plot_filetypes(ft_count, ft_size, top_n=50):
        #sort both dics by value and plot them as bar chart
        ft_count = dict(sorted(ft_count.items(), key=lambda item: item[1], reverse=True))
        ft_size = dict(sorted(ft_size.items(), key=lambda item: item[1], reverse=True))

        #print number of filetypes which have less than 4 occurernces
        print("Number of filetypes with less than 4 occurernces: ", len([x for x in ft_count.values() if x < 4]))
        
        ft_count = dict(list(ft_count.items())[:top_n])
        ft_size = dict(list(ft_size.items())[:top_n])
        
        #plot both dicts as bar chart horizontally
        fig, ax = plt.subplots(1,2, figsize=(10,10))
        ax[0].barh(list(ft_count.keys()), list(ft_count.values()))
        ax[0].set_title("Filetypes count")
        ax[1].barh(list(ft_size.keys()), list(ft_size.values()))
        ax[1].set_title("Filetypes size")
        plt.show()
    


class DataverseParser(ParserBase):
    # https://guides.dataverse.org/en/5.13/api/search.html
    base_dir = """C:/Users/Luki/Documents/Studia/Inzynierka/AI-based-dataset-recommendation-system/data/Dataverse/"""
    response_filename = "responses.ndjson"
    def __init__(self, url="https://dataverse.harvard.edu/api/search"):
        self.url = url
        self.data = None
        self.data_dict = None
    
    #local column names from parser ecosystem 
    BASE_COLUMN_NAMES = ["doi", "download_time", "created", "updated", "version", "title",
                        "authors", "description", "tags", "filetypes", "filepaths"]
    
    #native column names from dataverse
    ORIGINAL_COLUMN_NAMES = ["global_id","download_time", "createdAt", "updatedAt", "versionId", "name", 
                             "authors", "description", "keywords", "filetypes", "filepaths"]
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

    """                 """
    """LOW LEVEL METHODS""" 
    """                 """
    def load_filetypes_from_responses_file(self, response_file_name: str = response_filename,regex=True, load_files_to_data=True):
        dict_of_filetypes_count = {}
        dict_of_filetypes_size = {}
        
        if response_file_name is None :
            print("No response file name given")
            return None
        
        def is_valid_file_extension(extension:str):
            if regex == False:
                return True 
            #check if does not contain any special characters is not empty and is not a number and is maximum 10 characters long
            return re.match(r"^[a-zA-Z0-9]{1,10}$", extension) is not None
        
        if 'files' not in self.data.columns and load_files_to_data == True:
            self.data['files'] = [[] for _ in range(len(self.data))]

        with open(dp.base_dir + response_file_name, "r") as f:
            #iterate over all lines load json
            for line in tqdm(f):
                line = json.loads(line)
                #line is a dict with "data" and "doi" as keys
                doi = line["doi"]
                #iterate over all files
                try:
                    line["data"]["latestVersion"]["files"]
                except KeyError as e:
                    print(e)
                    print(line)
                    continue 
                
                f_name_taken = 0
                f_format_taken = 0
                f_storage_taken = 0
                
                cur_record = None
                if load_files_to_data:
                    cur_record = self.data.loc[self.data["doi"] == doi, "files"].iloc[0]

                for file in line["data"]["latestVersion"]["files"]:
                    f_format = file["dataFile"]["filename"]
                    storage_identifier = file["dataFile"]["storageIdentifier"]
                    
                    if "." in f_format and is_valid_file_extension(f_format.split(".")[-1]):
                        f_format = f_format.split(".")[-1]
                        f_name_taken+=1
                    elif "." in storage_identifier and is_valid_file_extension(storage_identifier.split(".")[-1]):
                        f_format = storage_identifier.split(".")[-1]
                        f_format_taken +=1
                    else:
                        f_format = file["dataFile"]["contentType"]
                        f_storage_taken +=1
                    
                    if load_files_to_data:
                        file_dict = {"name": file["dataFile"]["filename"], "type": f_format, "storage": storage_identifier, "size": file["dataFile"]["filesize"]}
                        cur_record.append(file_dict)
                        
                    
                    dict_of_filetypes_count[f_format] = dict_of_filetypes_count.get(f_format, 0) + 1
                    dict_of_filetypes_size[f_format] = dict_of_filetypes_size.get(f_format, 0) + file["dataFile"]["filesize"]
        if load_files_to_data:
            self.dict_of_filetypes_count = dict_of_filetypes_count
            self.dict_of_filetypes_size = dict_of_filetypes_size 
        print(f_name_taken, f_format_taken, f_storage_taken)
        return dict_of_filetypes_count, dict_of_filetypes_size

    """DEPRECATED
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
    """    

    
    #newline delimited json                
    def download_dataset_details(self, only_missing:bool = True, result_filename:str= None):
        def get_already_downloaded():
            regex_doi = re.Regex("doi\"\: \"([^\"]*)\"")
            already_downloaded = set()
            with open(result_filename, "r") as f:
                for line in f:
                    #find with regex \"doi\"\: \"([^\"]*)\"
                    already_downloaded.add( regex_doi.search(line).group(1))
            return already_downloaded
        
        excluded_dois = set()
        result_filename = self.base_dir +"files_info/"+  ( result_filename if result_filename is None else "responses.ndjson")
                
        #load already downloaded dois
        if os.path.exists(result_filename) and only_missing:
            excluded_dois.update(get_already_downloaded())
        
        #load excluded dois
        if os.path.exists(self.base_dir + "excluded_dois.txt"):
            with open(self.base_dir + "excluded_dois.txt", "r") as f:
                for i in f.read().splitlines():
                    excluded_dois.add(i)
         
        dois = self.data["doi"].tolist()
        dois = [i for i in dois if i not in excluded_dois]
        
        chunk_size = 1000
        chunks = [dois[i:i + chunk_size] for i in range(0, len(dois), chunk_size)]
        chunk_num = 0

        for chunk in tqdm_asyncio(chunks):
            try:
                responses = get_dataverse_files(chunk, dataverse_key)

            except Exception as e:
                print(e)
            #Append responses to one large json file
            with open(result_filename, "rw") as f:
                #open log file
                with open(self.base_dir + "log.txt", "a") as log:
                    for response in responses:
                        if response is None:
                            continue
                        if "status" in response and response["status"] == "ERROR":
                            log.write(response["message"] + "\n") 
                            excluded_dois.add(response["data"]["persistentId"])
                            continue
                        
                        try:
                            #remove newlines from response
                            response = json.loads(response.replace("\n", " "))
                            #add doi as key to response
                            response["doi"] = response["data"]["protocol"] + ":" + response["data"]["authority"] + "/" + response["data"]["identifier"]
                            #add date of download
                            response["download_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            #add response to file 
                            f.write(json.dumps(response, allow_nan=True, indent=None) + "\n")
                        except OSError as e:
                            #print whole exception information
                            print(e)
                            break
                        except KeyError as e:
                            print(e)
                            print(response)
                            continue 
    def download_search_results(self, start=1, per_page=1000, debug=False):
        dataverse_metadata = []
        start = 1
        all =   300000 
        headers = {"X-Dataverse-key": dataverse_key}
        progress_bar = None
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
            if start == 1:
                all = response_json['data']["total_count"] 
                progress_bar = tqdm(total=all)
            start += per_page
            progress_bar.update(per_page)
        progress_bar.close()
        return dataverse_metadata
    
    def correct_types(self,data:pd.DataFrame, in_place=False, native_column_names=False):
        data_tmp = None
        #convert columns to correct types
        if(in_place):
            data_tmp = data
        else:
            data_tmp = data.copy(True)
    
        if native_column_names:
            #columns with native names from ORIGINAL_COLUMN_NAMES
            data_tmp["download_time"] = pd.to_datetime(data["download_time"])
            data_tmp["createdAt"] = pd.to_datetime(data["createdAt"])
            data_tmp["updatedAt"] = pd.to_datetime(data["updatedAt"])
            data_tmp["versionId"] = pd.to_numeric(data["versionId"])
            data_tmp["fileCount"] = pd.to_numeric(data["fileCount"])
            data_tmp["majorVersion"] = pd.to_numeric(data["majorVersion"])
            data_tmp["minorVersion"] = pd.to_numeric(data["minorVersion"])
            data_tmp["doi"] = data["doi"].astype(str)
            data_tmp["title"] = data["title"].astype(str)
            data_tmp["description"] = data["description"].astype(str)
        else:
            #columns with parser names from BASE_COLUMN_NAMES ["doi", "download_time", "created", "updated", "version", "title", "authors", "description", "tags", "filetypes", "filepaths"]
            data_tmp["download_time"] = pd.to_datetime(data["download_time"])
            data_tmp["created"] = pd.to_datetime(data["created"]) 
            data_tmp["updated"] = pd.to_datetime(data["updated"])
            data_tmp["version"] = pd.to_numeric(data["version"])
            data_tmp["title"] = data["title"].astype(str)
            data_tmp["authors"] = data["authors"].astype(str)
            data_tmp["description"] = data["description"].astype(str)
            data_tmp["tags"] = data["tags"].astype(str)
            data_tmp["doi"] = data["doi"].astype(str)

        #convert authors to list of strings
        return data_tmp  
    

    """                    """
    """HIGH LEVEL FUNCTIONS"""
    """                    """
    def download(self,start=1, per_page=1000, debug=False):
        print("Downloading metadata from dataverse...")
        dataverse_metadata = self.download_search_results( start, per_page, debug)
        print("Downloaded " + str(len(dataverse_metadata)) + " datasets")
        print("Parsing metadata...")
     
        for data in tqdm(dataverse_metadata):
            data["download_time"] = datetime.now()
            data["filetypes"] = []
            data["filepaths"] = []
            data["files"] = []
            
        
        self.data_dict = dataverse_metadata
        self.data = self.to_dataframe(dataverse_metadata)
        self.data = self.convert(self.data)
        print("Downloading dataset details...")
        self.download_dataset_details()
        print("Parsing dataset details... Approximately 10min")
        self.load_filetypes_from_responses_file() 

    def convert(self, data:pd.DataFrame, in_place=False):
        filtered = self.filter_out(data)
        column_name_map = dict(zip(self.ORIGINAL_COLUMN_NAMES,self.BASE_COLUMN_NAMES))
        filtered = filtered.rename(columns=column_name_map) 
        filtered = self.correct_types(filtered,in_place, native_column_names=False)
        return filtered 
   
    def save(self, filename:str):
        ParserBase.save(self, self.base_dir + filename)
        
    def create_embedding(self):
        pass
    
    def should_update(self, *args, **kwargs) -> bool:
        return False
    
    def update(self, *args, **kwargs):
        pass

    """ UTILITIES """ 
    def export_scibert_input(self, name:str = "scibert_input"):
        #export to json file
        self.data[["doi", "title", "description","tags"]].to_json(self.base_dir + name + ".json", orient="records")
    
    def load_scibert_input(self, name:str = "scibert_input"):
        #load from json file
        self.data = pd.read_json(self.base_dir + name + ".json", orient="records")
    
    def print_records(self, how_many = 2, random = False,columns:list[str]=[]):
        print("Printing for each collumn non null records")
        if columns == []:
            columns = self.data.columns
        for col in columns:
            #print so it shows whole value and not only first 50 chars
            pd.set_option('display.max_colwidth', None)

            try:
                tmp_data = dp.data[dp.data[col].map(lambda d: len(d) > 0)]
                if len(tmp_data) == 0:
                    print("No records for column " + col + "\n\n")
                    continue
                if random:
                    print(col,'\n',tmp_data[col].dropna().sample(how_many),'\n\n')
                else:
                    print(col,'\n',tmp_data[col].dropna().head(how_many),'\n\n') 
            except KeyError:
                print("Column " + col + " does not exist")
                continue

        

if __name__ == "__main__":
    dp = DataverseParser()
    #dp.download(debug=True)
    #dp.save("dataverse")
    dp : DataverseParser = dp.load(dp.base_dir+"dataverse_w_filetypes")
    dp.data =dp.convert(dp.data)
    dp2 = DataverseParser()
    dp2.data = dp.data
    dp2.data_dict = dp.data_dict
    dp2.load_filetypes_from_responses_file("combined_responses.ndjson")
    dp2.save("dataverse_export_to_merge")
    exit(0)
    print(dp.data.columns)
    dp.print_records(3, random=True, columns=["filetypes", "filepaths", "files","doi"])
    input("Press enter to continue")
    #generate brief report about this dataframe
    
    
    dp.print_records(3, random=True, columns=["filetypes", "filepaths", "files","doi"])
    dp.save("dataverse_w_files_column")
    exit(0)
    
    dict_of_filetypes_count = {}
    dict_of_filetypes_size = {}
    
    #open ndjson file as stream over lines
    
    
    new_dict_of_filetypes_count, new_dict_of_filetypes_size = dp.load_filetypes_from_responses_file("combined_responses.ndjson") 
    noregex_dict_of_filetypes_count, noregex_dict_of_filetypes_size = dp.load_filetypes_from_responses_file("combined_responses.ndjson", regex=False) 

    #print all filetypes and their count

    dict_of_filetypes_count = dp.dict_of_filetypes_count 
    dict_of_filetypes_size = dp.dict_of_filetypes_size 
    #plot_filetypes(dict_of_filetypes_count, dict_of_filetypes_size)
    plot_filetypes(new_dict_of_filetypes_count, new_dict_of_filetypes_size)
    plot_filetypes(noregex_dict_of_filetypes_count, noregex_dict_of_filetypes_size)
    
    
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
    """

    
    #dp.data =dp.convert(dp.data)
    #dp.export_scibert_input() 
    
    #convert files from files_info to  one ndjson format file with doi and download time
    #iterate over all files in directory
    
    
        
    """dp_withfiles = dp.get_filetype_information(dp_converted, "doi")
    dp.data = dp_withfiles
    dp.save( "dataverse_withfiles")
    print(dp_withfiles["files"].head())
    """
    #dp.load_file_information() 
    #dp.download_file_information()
    #drop records with empty or very short descriptions
    #dp.data = dp.data[dp.data["description"].str.len() > 10]
    
    #plot histogram of description lengths and assign axes labels limit x axis to 1000    
    #plt.hist(dp.data["description"].str.len(), bins=1000)
    #plt.xlabel("Description length")
    #plt.ylabel("Number of records")
    #plt.xlim(0,1000)
"""
    def axe( paths:list[str], id:int ):
        print("running axe with id: " + str(id) + " and " + str(len(paths)) + " files")
        with open(base_dir + "files"+str(id)+".ndjson", "w") as f:
            for entry in paths:
                with open(entry, "r") as small_file:
                    if small_file is None:
                        print("Error: could not open file: " + entry.path)
                        continue
                    if small_file.read() == "":
                        print("Error: file is empty: " + entry.path)
                        continue
                    small_file.seek(0)
                    if small_file.read() == "null":
                        print("Error: file is null: " + entry.path)
                        continue
                    small_file.seek(0)
                    try:
                        
                        small_file_json = json.load(small_file)
                        #remove newline characters in json
                        small_file_json = {k: v.replace("\n", " ") if isinstance(v, str) else v for k, v in small_file_json.items()} 
                        small_file_json["doi"] = small_file_json["data"]["protocol"] + ":" + small_file_json["data"]["authority"] + "/" + small_file_json["data"]["identifier"]
                        #add date of download
                        small_file_json["download_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        #add response to file 
                        f.write(json.dumps(small_file_json, allow_nan=True, indent=None) + "\n")
                    except json.decoder.JSONDecodeError as e:
                        print("Error: could not parse json: " + entry.path)
                        print(e)
                        exit(1)

    def json_pick():
        with open(dp.base_dir + "responses.ndjson", "w") as result_file:
            with os.scandir(dp.base_dir + "files_info") as dir:
                #run this concurrently with 8 threads 
                with ThreadPoolExecutor(max_workers=8) as executor:
                    #iterate over all files in directory
                    #add semaphore t
                    #split files into 8 chunks
                    dir_chunks = np.array_split(list(dir), 8)
                    futures = []
                    iterator = 1
                    for entry in tqdm(dir_chunks):  
                        #submit each chunk to executor 
                        futures.append(executor.submit(axe, entry, iterator ))
                        iterator+=1 
                    #wait for all futures to finish
                    for future in tqdm(futures):
                        future.result()  
                               
    json_pick()
"""