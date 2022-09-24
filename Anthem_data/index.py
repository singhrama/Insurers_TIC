"""
	Desc: Script for generating small chunks of CSV 
	by parsing large Anthem json.gz files
	Date: 09/05/2022
"""
import os
import sys
import gzip
import json
import pandas as pd

def process_data(json_list, batch, out_folder):
    agg_out = []
    for d in json_list:
        agg_out.extend(parse_data(d))
    df = pd.DataFrame(agg_out)
    f_name = out_folder + "/Chunk_" + str(batch) + ".csv"
    df.to_csv(f_name, index=False)
    print("Generated file:",f_name)

def parse_data(d):
    meta = {}
    meta["billing_code"] = d.get("billing_code")
    meta["billing_code_type"] = d.get("billing_code_type")
    meta["billing_code_type_version"] = d.get("billing_code_type_version")
    meta["name"] = d.get("name")
    
    n_rates = d.get("negotiated_rates")
    output = []
    for item in n_rates:
        item.update(meta)
        item.update(item["negotiated_prices"][0])
        del item["negotiated_prices"]
        output.append(item)
    return output

if __name__ == "__main__":
	
	input_config = json.load(open("config.json","r"))
	input_path = input_config.get("input_file_path","")
	output_folder = input_config.get("output_folder_path","")
	total_chunks = input_config.get("total_chunks",1)
	entities_per_chunk = input_config.get("num_entities_per_chunk",-1)
	billing_code = input_config.get("billing_code","")

	
	output_folder = output_folder+"/"+input_path.split("/")[-1].replace(".json.gz","")

	os.makedirs(output_folder, exist_ok=True)
	
	batch_counter = 1
	final_output = []
	with gzip.open(input_path, 'rb') as f:
		temp_out = []
		flag = False
		for line in f:
			text = line.decode("utf-8")
			if "in_network" in text:
			    flag = True
			    continue
			if flag:
				text = text.strip()
				text = text.strip(",")
				data = json.loads(text)
				if len(temp_out) == entities_per_chunk:
					process_data(temp_out, batch_counter, output_folder)
					temp_out = []
					batch_counter += 1
				if billing_code != "":
					if data["billing_code"] == billing_code:
						temp_out.append(data)
					else:
						pass
				else:
					temp_out.append(data)

			if batch_counter == total_chunks:
			    break
