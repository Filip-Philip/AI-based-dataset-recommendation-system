from typing import Dict
from datetime import datetime
from pathlib import Path
#deprecated
class DataCluster:
    def __init__(self,data:Dict):
        self.data = data
    def __init__(self,filepath:Path):
        #load from specified file 
        raise NotImplementedError
   
    
    """
    Save cluster to file:
        idea? -> first line holding timestamp,
                 from second line onwards, just normal csv deserializing dict with data 
    """
    def save():
       raise NotImplementedError
   
     