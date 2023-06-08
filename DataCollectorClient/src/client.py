import datetime
import logging as log
from services.zenodo_parser import ZenodoParser
from services.dataverse_parser import DataverseParser
import json
import os

# loading config with filepaths and options how to handle parsers
# creating filetree for parsers
# loading parsers from pickle files
# running download and update methods

if __name__ == "__main__":
    #CONFIG
    WORKING_DIR = os.path.dirname(os.path.realpath(__file__))
    CONFIG = {}
    CONFIG["WORKING_DIR"] = WORKING_DIR
    CONFIG["DATA_DIR"] = os.path.join(WORKING_DIR,"data")
    CONFIG["LOG_DIR"] = os.path.join(WORKING_DIR,"logs")
    CONFIG["LOG_FILE"] = os.path.join(CONFIG["LOG_DIR"],"client.log")
    CONFIG["LOG_LEVEL"] = log.DEBUG
    CONFIG["LOG_FORMAT"] = "%(asctime)s %(levelname)s %(message)s"
    CONFIG["LOG_DATE_FORMAT"] = "%Y-%m-%d %H:%M:%S"
    CONFIG["LOG_FILEMODE"] = "w"
    CONFIG["LOG_ENCODING"] = "utf-8"

    #PARSERS
    CONFIG["PARSERS"] = []
    CONFIG["PARSERS"].append({"name":"ZenodoParser","enabled":True, "update":True, "update_interval":1, "pickle_fname":"ZenodoParser.pickle", "debug":True, "debug_log_file":"ZenodoParser.log"})
    CONFIG["PARSERS"].append({"name":"DataverseParser","enabled":True, "update":True, "update_interval":1, "pickle_fname":"DataverseParser.pickle" , "debug":True, "debug_log_file":"DataverseParser.log"})




    #TODO: LOAD FROM CONFIG YAML FILE


    #check if there is data directory
    if not os.path.exists(os.path.join(WORKING_DIR,"data")):
        os.mkdir(os.path.join(WORKING_DIR,"data"))

    parsers = {}

    #prepare filetree and load parsers
    for parser_config in CONFIG["PARSERS"]:
        if not parser_config["enabled"]:
            continue
        else:
            parser_name = parser_config["name"]
            parser = globals()[parser_name]()
            parsers[parser_name] = parser
            #create parser directory in data directory if not exists
            parser_dir = os.path.join(CONFIG["DATA_DIR"],parser_name)
            if not os.path.exists(parser_dir):
                os.mkdir(parser_dir)
            #find in parser dir pickle file
            pickle_fname = parser_config["pickle_fname"]
            pickle_path = os.path.join(parser_dir,pickle_fname)
            if os.path.exists(pickle_path):
                parser = parser.load(pickle_path)
                parsers[parser_name] = parser
            parser.base_dir = parser_dir
            parser.debug = parser_config["debug"]
            parser.debug_log_file = os.path.join(CONFIG["LOG_DIR"],parser_config["debug_log_file"])
            #create parser log file
            if not os.path.exists(parser.debug_log_file):
                with open(parser.debug_log_file,"w") as f:
                    f.write("")
        
            parser.debug_log(parser.debug,"Parser {} loaded".format(parser_name))

    print("Parsers loaded: {}".format(parsers.keys()))

    for parser_name, parser in parsers.items():
        print("Running parser {}".format(parser_name))

        
        #DOWNLOAD/UPDATE
        
        #check if parser should be updated
        last_update = parser.last_update 
        time_elapsed =  datetime.datetime.now() - parser.last_update
        print("Time elapsed: {}".format(time_elapsed)) 
        if parser.last_update is None or  parser.last_update  < CONFIG["PARSERS"][parser_name]["update_interval"]:
            parser.update()
            parser.save(os.path.join(parser.base_dir,parser.pickle_fname))
        else:
            print("Parser {} update is not required by timeinterval".format(parser_name))
        
        #CONVERT
        
        parser.data = parser.convert(parser.data)
        parser.save(os.path.join(parser.base_dir,parser.pickle_fname))
        #TODO: think if that flow is ok
        
            
    #TODO: concatenate all dataframes from all parsers / or create embeddings and store them        
    #TODO: check if there is pickled parser in current directory
        """"""
        

        
