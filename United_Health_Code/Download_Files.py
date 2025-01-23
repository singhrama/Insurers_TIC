# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import sys
import requests
import csv
import pathlib
import sys
import glob
import duckdb
import option_util as util
import pandas
from tabula.io import read_pdf
import tabula
import os
import json
from tqdm import tqdm

# Read file from command-line
def setup_logging(inti_file, chunkid):

    ###Gets or creates a logger
	logger = logging.getLogger(__name__)  
    
    # set log level
	logger.setLevel(logging.INFO)

    # define file handler and set formatter
    #file_handler = logging.FileHandler('logfile.log')
	out_files = os.path.join(inti_file, 'log')
	if not os.path.isdir(out_files): os.mkdir(out_files)
	
	logfilepath = out_files + '/'+ 'logfile_chunkid_'+ str(chunkid) + "_" +str(datetime.datetime.now()).replace(":","_") + '.log'
        
	file_handler = logging.FileHandler(logfilepath)
	formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
	file_handler.setFormatter(formatter)

    # add file handler to logger
	logger.addHandler(file_handler)
    
	return logger
	
def read_content(p, cols, codes):
    """
    Parse Humana in-network file for specific billing codes.

    Args:
        p: pathlib.Path object for humana file
        cols: list of columns to extract from the file
        codes: list of billing codes to search for

    Returns:
        pandas.DataFrame with relevant rows and columns from the file.
        If the file is empty (<0.5MB) or malformed, return None.

    """

    def has_content(p):
        """File > 0.5 MB"""
        return p.stat().st_size / 1e6 > 0.5

    df = None
    if has_content(p):
        try:
            df = (
                pandas.read_csv(p, dtype=str, usecols=cols)
                .query("BILLING_CODE.isin(@codes)")
                .assign(NPI=lambda df: df.NPI.str.split(","))
                .explode("NPI", ignore_index=True)
            )
        except EOFError:
            df = None
    return df


# -

def qdf(con, sql):
    """Query pandas dataframes using duckdb

    Args:
        con: duckdb connection
        sql: sql string

    Returns:
        pandas.DataFrame
    """
    return con.execute(sql).df()

def check_filename_exist(inti_path,filename,ologger):
	otrack = inti_path + '/' + "File_names_tracker.json"
	if os.path.exists(otrack):
		with open(otrack, 'r') as jf:
#             print(otrack)
			data = read_json(jf)

		if not (data.get(filename)):
			return True    # If new cusip 
		else:
			ologger.info(filename + '_Already_Downloaded')
			return False   # If cusip already downloaded
	else:
		return True  # If new cusip

def read_json(jf):
	i=0
	for i in range(5):
		try:
			data = json.load(jf)
			return data
		except ValueError:
			time.sleep(10)
			i+=1

def add_cusip_exist(inti_path,filename,ologger):
	otrack = inti_path + '/' + "File_Names_Tracker.json"
	if os.path.exists(otrack):
		with open(otrack, 'r') as jf:
			data = read_json(jf)
#             print(otrack)
  
		if not (data.get(filename)):
			data.update({filename:filename})
			with open(otrack, 'w') as jf:
				json.dump(data, jf)
				jf.close()
				
	else:
		with open(otrack, 'w') as jf:
			json.dump({filename:filename}, jf)
			jf.close()

#Input Parameters			
save_path = '/N/project/TIC/Humana/data_10_2022'
json_files = '/N/project/cryptocurrency_data/TIC_DATA/Batch_Process_data_download/File_Name_Run_JSON/'   
inti_path = '/N/project/cryptocurrency_data/TIC_DATA/Batch_Process_data_download'
URL = 'https://developers.humana.com/Resource/DownloadPCTFile?fileType=innetwork&fileName=2022-10-26_1_in-network-rates_0000000'

#Read Arguments from Batch Process
args = util.get_args()
chunk_id = args['chunkid']
print("Chunk_id : ",chunk_id)

#Read JSON file to obtain one Chunk of File Names
with open(json_files + "File_Names_" + str(chunk_id) + '.json', 'r') as jf:
	new_files = json.load(jf)

#Change JSON file format to List
new_files=list(new_files)

#Loop to download files from Chunks
for file in tqdm((new_files)):
	ofile = pathlib.Path(save_path + file + '.csv.gz')
	reqo = requests.get(URL + file + '.csv.gz') 
	ofile.write_bytes(reqo.content)
