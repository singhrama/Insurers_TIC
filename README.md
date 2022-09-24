Parsing Anthem “in_network” json.gz files


Approach:

* As we are unable to unzip the Json.gz files due to memory constraints, we came up with a solution using Python’s Gzip library. 


Procedure/Algorithm:

1. Gzip library provides functionality to read contents of any given zip file without uncompressing it. 
2. We are iterating the gzip file line by line. Based on several predefined conditions, such as, looking up for keywords like “in_network”, “provider_reference”, we are filtering the data which has the required information.
3. After identifying the data, we are parsing the json object (present in string format) to extract required fields and map these fields with respective billing information.
4. During the filtering process, we also provide functionality to filter out the data based on the given billing_code.
5. After parsing, we are grouping several billing_codes together and storing this data into smaller CSV chunks (this script provides flexibility to choose the number of billing_codes to include in a chunk).


Steps to run the code:

* To run the script, we need to provide the following information in the config.json file located in the same directory.
1. Input Path: Path to the json.gz file which needs to be parsed.
2. Output Path: Path to the folder where output csv chunks should be stored.
3. Billing Code: This is optional, if provided, will generate only the chunks having this code.
4. Number of billing codes: Number of different billing codes per chunk.
5. Number of Chunks: Number of sample chunks required from the input file.
* Location: /N/project/TIC/Anthem/parse_data
* Command: python3 unzip_in_network_gz_main.py
           python3 index.py config.json
