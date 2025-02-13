# https://catalog.archives.gov/id/5573655
# see schema for series, then file unit or item
# plan: 
# would be good to take command line args for e.g. parent id
# use api to get items in collection by parent id https://catalog.archives.gov/api/v2/records/parentNaId/{id}
# write out json verbatim
# child records are in $.body.hits.hits array
# records have s3 links in .digitalObjects.objectUrl
# pretty deeply nested
# api docs: https://catalog.archives.gov/api/v2/api-docs/
# also listed in aws open data registry https://registry.opendata.aws/nara-national-archives-catalog/

import requests
import pandas as pd  
from dotenv import load_dotenv
import os
import json
from benedict import benedict
from pathlib import Path
import re
# from concurrent.futures import ProcessPoolExecutor as Pool 
import multiprocessing
from tqdm.contrib.concurrent import process_map
import argparse

num_cpu = multiprocessing.cpu_count() - 1

def to_snake_case(x):
    return re.sub(r'(\s|[^\w])+', '_', x.lower())

def get_records(obj, 
                path, 
                key):
    x = benedict(obj)[path]
    if key in x:
        # return x.subset(['recordType', 'title', 'naId', key]) 
        return {
            'meta': x.subset(['recordType', 'title', 'naId']),
            'objs': x[key]
        }
    else:
        return None
    
def recs_to_df(obj, 
               path = ['_source', 'record'], 
               key = 'digitalObjects'):
    recs = get_records(obj, path, key)
    if recs is not None:
        # meta_df = pd.DataFrame([recs['meta']])
        recs['objs'] = pd.DataFrame(recs['objs'])
        return recs
    else:
        return None
    
def prep_output(item):
    # make directory from meta, write df to csv
    # output paths based on dir
    meta = item['meta']
    df = item['objs']
    id = meta['naId']
    title = to_snake_case(meta['title'])
    direc_name = f'{id}_{title}'
    direc = Path('output') / direc_name
    direc.mkdir(parents=True, exist_ok=True)
    csv = direc / f'records_{id}.csv'
    # write csv
    df.to_csv(csv, index=False)
    
    df['path'] = df['objectFilename'].apply(lambda x: direc / x)
    return df[['objectUrl', 'path']]

def download_single(url, path):
    # dl single file
    if not path.exists():
        resp = requests.get(url)
        if resp.status_code == requests.codes.ok:
            with open(path, 'wb') as file:
                file.write(resp.content)
    
def fetch_records(id, url, path_out, key_name, limit = 50):
    hdrs = {
        'Content-Type': 'application/json',
        'x-api-key': os.getenv(key_name)
    }
    prms = {
        'limit': limit
    }
    resp = requests.get(url, headers = hdrs, params = prms)
    print(f'request status: {resp.status_code}')
    resp.raise_for_status()
    
    with open(path_out, 'w') as file:
        json.dump(resp.json(), file)
        print(f'results written to {path_out}')
    return benedict(resp.json())
    
def get_id_from_args():
    prsr = argparse.ArgumentParser(
        prog = 'NARA scraper',
        description = 'Download digitized NARA records from parent ID'
    )
    prsr.add_argument('id', default = '5573655')
    args = prsr.parse_args()
    return args.id

def main():
    # load .env file for api key
    load_dotenv()
    
    # get base id from args
    # base_id = get_id_from_args()
    base_id = '5573655'
    base_url = f'https://catalog.archives.gov/api/v2/records/parentNaId/{base_id}'
    json_out = f'results_{base_id}.json'
    
    # make api request for records, return benedict object, pluck out hits
    # in the process, write out results of api call to json
    records = fetch_records(base_id, base_url, json_out, 'NARA_KEY')[['body', 'hits', 'hits']]
    
    # extract digitalObjects where they exist
    items = [recs_to_df(x) for x in records]
    # items is list of dicts, with meta = dict, objs = df--bind into single df
    items_df = pd.concat([prep_output(item) for item in items if item is not None])
    
    # download files in parallel with cute progress bar
    process_map(download_single, 
                items_df['objectUrl'], items_df['path'], 
                max_workers = num_cpu,
                chunksize = 10)
    
    
########## main body
if __name__ == '__main__':
    main()
    