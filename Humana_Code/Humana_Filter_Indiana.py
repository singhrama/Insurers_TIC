# -*- coding: utf-8 -*-
#!/usr/bin/env python3

import sys
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
                pandas.read_csv(p, sep = '~', dtype=str, usecols=cols)
                .query("BILLING_CODE.isin(@codes) & NEGOTIATION_ARRANGEMENT =='ffs' & NEGOTIATED_TYPE =='negotiated'")
                .assign(NPI=lambda df: df.NPI.str.split(","))
                .explode("NPI", ignore_index=True)
            )
        except EOFError:
            df = None
    return df			
                
                
#Input Parameters as File Paths
os.chdir('/N/project/TIC/Humana/data_10_2022')
save_path = '/N/project/TIC/Humana/99213_ffs_negotiated'
json_files = '/N/project/cryptocurrency_data/TIC_DATA/Chunkify/File_Name_Run_JSON/'   
inti_path = '/N/project/cryptocurrency_data/TIC_DATA/Chunkify'

# Adjust to get desired columns from csv file
cols = ["NPI", "NEGOTIATION_ARRANGEMENT","BILLING_CLASS", "BILLING_CODE", "NEGOTIATED_RATE","NEGOTIATED_TYPE", "TIN", "TYPE", "BILLING_CODE_TYPE"]

#Obtain arguments from Batch process Intialization
args = util.get_args()
chunk_id = args['chunkid']
print("Chunk_id : ",chunk_id)

#Filespaths to process from Chunk
with open(json_files + "File_Names_" + str(chunk_id) + '.json', 'r') as jf:
	new_files = json.load(jf)

#Change JSON file format to List
new_files=list(new_files)

#Define Billing Code to Extract
bl_code = ['99213']

for file in tqdm((new_files)):
    
    infile = pathlib.Path(file)
    #Method call to split NPIs, filter NEGOTIATION_ARRANGEMENT as 'ffs' & NEGOTIATED_TYPE as 'negotiated'
    df_humana = read_content(infile, cols, bl_code)
    #Save the processed DataFrame as CSV
    df_humana.to_csv(save_path + '/' + file.rsplit('/',1)[1], index=False, sep=",")
