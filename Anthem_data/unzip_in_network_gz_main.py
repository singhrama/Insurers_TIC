import gc
import gzip
import os
import json
import numpy as np
import time
import pandas as pd
import gc


'''
The program will save splitted files into multiple small .npy files (in-network list) 
and a 'meta.npy' file which contains other information, such as provider_reference)

In default, the program will create a sub foldeer with name of the gz file, and save all splitted files in it.

'''


#################################################
def parsing_nego_arrange(save_path, subfile_name, sub_file=None):
    if sub_file is None:
        sub_file = np.load(save_path + subfile_name, allow_pickle=True).item()
    print(sub_file.keys())
    # dict_keys(['negotiation_arrangement', 'name', 'billing_code_type', 'billing_code_type_version', 'billing_code', 'description', 'negotiated_rates'])
    for k in list(sub_file.keys()):
        if (type(sub_file[k]) is list) or (type(sub_file[k]) is dict):
            print(k, len(sub_file[k]))
        else:
            # print(k, type(sub_file[k]))
            pass
        '''
    # negotiation_arrangement <class 'str'>
    # name <class 'str'>
    # billing_code_type <class 'str'>
    # billing_code_type_version <class 'str'>
    # billing_code <class 'str'>
    # description <class 'str'>
    # negotiated_rates 32760
    '''
    nr = sub_file['negotiated_rates']
    print('number of negotiate_rate items:', len(nr))
    # print(nr[0].keys())
    # dict_keys(['provider_references', 'negotiated_prices'])
    # after check, each of sub_file['negotiated_rates'][x]['negoriated_prices'] contains only one dicttionary (for x in 1,..., 32760)
    return sub_file


def convert_negotiated_arrangement_npy_to_csv(subfile_i, save_path):
    billing_code = subfile_i['billing_code']
    nr_table = pd.DataFrame(columns=['provider_reference', 'negotiated_prices', 'billing_class'])
    for j in range(len(subfile_i['negotiated_rates'])):
        for k in range(len(subfile_i['negotiated_rates'][j]['negotiated_prices'])):
            nr_table.loc[len(nr_table)] = [subfile_i['negotiated_rates'][j]['provider_references'][0],
                                           subfile_i['negotiated_rates'][j]['negotiated_prices'][k]['negotiated_rate'],
                                           subfile_i['negotiated_rates'][j]['negotiated_prices'][k]['billing_class']]
    nr_table['billing_code'] = billing_code
    nr_table.to_csv(save_path + '{}.csv'.format(billing_code), index=False)


########################################
# parsing meta information
def parse_meta_info(save_path):
    with open(save_path + 'meta.txt', 'r') as f:
        line = f.readlines()
    meta_text = ''
    for j in range(len(line)):
        if line[j].startswith('"provider_references'):
            provider_reference_i = j
            break
        else:
            meta_text += line[j]
    provider_reference_line = line[provider_reference_i]
    provider_reference_dict = json.loads(provider_reference_line[23:-2])  # first 23 chars: "provider_references":
    print(len(provider_reference_dict))
    # 66367
    # provider_reference_dict[0] = {'provider_groups': [{'npi': [1841361052], 'tin': {'type': 'ein', 'value': '582218877'}}], 'provider_group_id': 0}
    meta_dict = json.loads(meta_text[:-3] + '}')
    meta_dict['provider_reference'] = provider_reference_dict
    return meta_dict


def sub_provider_groups_table(pg):
    # pg_table = pd.DataFrame(columns=['npi', 'tin.type', 'tin.value'])
    pg_table = {'npi':[], 'ein.value':[]}
    for pgi in pg:
        for npi_i in range(len(pgi['npi'])):
            pg_table['npi'].append(pgi['npi'][npi_i])
            pg_table['ein.value'].append(pgi['tin']['value'])
    return pg_table


def parse_provider_reference(save_path):
    meta_dict = parse_meta_info(save_path)
    pr_df = {'npi': [], 'ein.value': [], 'provider_group_id': []}
    for mi in meta_dict['provider_reference']:
        pg_table_i = sub_provider_groups_table(mi['provider_groups'])
        pg_table_i['provider_group_id'] = [mi['provider_group_id'] for j in range(len(pg_table_i['npi']))]
        pr_df['npi'] += pg_table_i['npi']
        pr_df['ein.value'] += pg_table_i['ein.value']
        pr_df['provider_group_id'] += pg_table_i['provider_group_id']
    pr_df = pd.DataFrame(pr_df)
    pr_df.to_csv(save_path + 'provider_reference_df.csv', index=None)


#################################################
def split_large_json_gz(data_path, file, save_path, parse_meta=True):
    """
    :param data_path: where the .json.gz file locate
    :param file: the name of .json.gz file
    :param save_path: where the splitted files should be stored
    """
    ts = time.time()
    try:
        os.makedirs(save_path)
    except:
        pass
    with gzip.open(data_path + file, 'rb') as f:
        i = 0
        text_file = []
        for line in f:
            row_str = line.decode("utf-8")
            if row_str.startswith('\t{\"negotiation_arrangement') or row_str.startswith('{\"negotiation_arrangement'):
                row_str2 = row_str[1:-2]
                # # Use the following code if want to save as npy file
                # rr = json.loads(row_str2)
                # np.save(save_path + 'file_{}_negotiation_arrangement.npy'.format(i), rr, allow_pickle=True)
                # # Use the following code if want to save as json file
                # # save as json file
                with open(save_path + 'file_{}_negotiation_arrangement.json'.format(i), 'w') as json_file:
                    json.dump(row_str2, json_file)
            else:
                text_file.append(row_str)
                with open(save_path + 'meta.txt', 'w') as f2:
                    f2.writelines('%s\n' % lin for lin in text_file)
                f2.close()
            i += 1
    print('time usage={} mins'.format((time.time() - ts) // 60))
    if parse_meta:
        parse_provider_reference(save_path)


#############################################
def parse_data(d):
    '''
    from index.py, credit by Pratheek
    '''
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


#################################################
def split_large_json_gz_v3(data_path, file, save_path, parse_meta=1, billing_code_list=None):
    """
    :param data_path: where the .json.gz file locate
    :param file: the name of .json.gz file
    :param save_path: where the splitted files should be stored
    :param billing_code_list: ['0001', '0002M',....], None for all
    allow specific billing code
    """
    ts = time.time()
    try:
        os.makedirs(save_path)
    except:
        pass
    if billing_code_list == None:
        flag = 0
    else:
        assert type(billing_code_list) is list
        flag = 1
        billing_code_list = [str(b) for b in billing_code_list]
    with gzip.open(data_path + file, 'rb') as f:
        i,j = 0,0
        text_file = []
        for line in f:
            row_str = line.decode("utf-8")
            if row_str.startswith('\t{\"negotiation_arrangement') or row_str.startswith('{\"negotiation_arrangement'):
                if row_str[0] != '{':
                    row_str = row_str[1:]
                while row_str[-1] != '}':
                    row_str = row_str[:-1]
                try:
                    rr = json.loads(row_str)
                except:
                    if row_str.endswith('}]}]}'):
                        rr = json.loads(row_str[:-2])
                if flag:
                    if rr['billing_code'] in billing_code_list:
                        # with open(save_path + 'file_{}_negotiation_arrangement.json'.format(i), 'w') as json_file:
                        with open(save_path + '{}.json'.format(rr['billing_code']), 'w') as json_file:
                            json.dump(row_str, json_file)
                        sub_df = parse_data(d=rr)
                        df = pd.DataFrame(sub_df)
                        df.to_csv(save_path + '{}.csv'.format(rr['billing_code']), index=False)
                else:
                    j += 1
                    with open(save_path + '{}.json'.format(rr['billing_code']),
                              'w') as json_file:
                        json.dump(row_str, json_file)
                    if j > 5:
                        break
            else:
                pass
                # text_file.append(row_str)
                # with open(save_path + 'meta.txt', 'w') as f2:
                #     f2.writelines('%s\n' % lin for lin in text_file)
                # f2.close()
            i += 1
    print('time usage={} mins'.format((time.time() - ts) // 60))
    if parse_meta:
        parse_provider_reference(save_path)


#############################################
def test_sample(billing_code_list1, file='IN_FHMCMEDIN10_3_3.json.gz', parse_meta=1):
    data_path = ''
    save_path = data_path + file.replace('.json.gz', '/')
    try:
        os.makedirs(save_path)
    except:
        pass
    split_large_json_gz_v3(data_path, file, save_path, parse_meta, billing_code_list=billing_code_list1)
    '''
    Established Patient Visit
 99213
   Colonoscopy
 45378
   Lower limb MRI
 73721
   Hip replacement surgery
 27130
   Lipid panel
 80061
   ED visit, high severity
 99285
    '''


#############################################
#############################################
if __name__ == '__main__':
    # Enter path of gz file
    # data_path = input('Enter Input Path: (ex: \'N/project/TIC/TICDataDownloadSiteJuly2022/United/WALMART/\')')
    #
    # # Enter gz file name
    # file = input('Enter File name: (ex: \'2022-07-01_UMR--Inc-_TPA_DOCTOR-ON-DEMAND-DCI_WALMART_-DOD_WAL_in-network-rates.json.gz\')')
    #
    # save_path = input('Enter Output Path:')
    # # save_path = data_path + file.replace('.json.gz', '/')
    # parse_meta = input('Do you want to parse meta information? (T/F)')
    # if parse_meta in ['T', 1, True]:
    #     parse_meta = True
    # else:
    #     parse_meta = False
    # split_large_json_gz(data_path, file, save_path, parse_meta)
    input_config = json.load(open('config_unzip_in_network.json','r'))
    file = input_config.get('file')
    parse_meta = input_config.get('parse_meta')
    billing_code_list1 = input_config.get('billing_code')
    test_sample(billing_code_list1=billing_code_list1, file=file, parse_meta=parse_meta)
