#!/usr/bin/env python
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
import multiprocessing
from tqdm.contrib.concurrent import process_map
import argparse

num_cpu = multiprocessing.cpu_count() - 1


def to_snake_case(x: str) -> str:
    '''
    Convert text to snake case.  

    Args:
        x (str): String to convert

    Returns:
        str: String with spaces and non-word characters replaced with underscores, and all characters in lower case.
    '''
    return re.sub(r'(\s|[^\w])+', '_', x.lower())


def get_records(obj: dict, path: list[str], key: str) -> dict | None:
    '''
    Subset a record dict to get its metadata and digital objects.  

    Args:
        obj (dict): Dict of a record
        path (list[str]): List of strings giving the path to the digital objects
        key (str): String giving the digital objects key

    Returns:
        dict | None: If keys are found, returns a benedict object with keys `meta` and `objs`. Else returns `None`.
    '''
    x = benedict(obj)[path]
    if isinstance(x, benedict) and key in x:
        return {
            'meta': x.subset(['recordType', 'title', 'naId']),
            'objs': x[key]
        }
    else:
        return None


def recs_to_df(obj: dict,
               path: list[str] = ['_source', 'record'],
               key: str = 'digitalObjects') -> dict | None:
    '''
    Generate metadata & a dataframe of digital objects data from a record dict.

    Args:
        obj (dict): Dict of a record
        path (list[str], optional): List of strings giving the path to the digital objects. Defaults to ['_source', 'record'].
        key (str, optional): String giving the digital objects key. Defaults to 'digitalObjects'.

    Returns:
        dict | None: If records are found, returns a dict with keys `meta` and `objs`, where objs is a dataframe. Else returns `None`.
    '''
    recs = get_records(obj, path, key)
    if recs is not None:
        recs['objs'] = pd.DataFrame(recs['objs'])
        # recs is now a dict of meta = benedict, objs = df
        return recs
    else:
        return None


def prep_dirs(base_output: str, id: str, title: str) -> Path:
    '''
    Prep directory for a collection's output files.

    Args:
        base_output (str): String giving the base output directory
        id (str): Collection ID used in naming the directory
        title (str): Collection title used in naming the directory

    Returns:
        Path: Returns path to the collection's directory
    '''
    title = to_snake_case(title)
    direc_name = f'{id}_{title}'
    direc_path = Path(base_output) / direc_name
    direc_path.mkdir(parents=True, exist_ok=True)
    return direc_path


def prep_output(output_dir: str, item: dict) -> pd.DataFrame:
    '''
    Setup records' directories, writing objects to a csv file.

    Args:
        output_dir (str): String giving output directory path
        item (dict): Dict of a record with keys `meta` and `objs`, where objs is a dataframe

    Returns:
        pd.DataFrame: Returns pandas dataframe with columns `objectUrl` and `path`, giving the url to download from and the path to write to.
    '''
    # make directory from meta, write df to csv
    # output paths based on dir
    meta = item['meta']
    df = item['objs']
    id = meta['naId']
    direc_path = prep_dirs(base_output=output_dir, id=id, title=meta['title'])
    csv_path = direc_path / f'records_{id}.csv'
    # write csv
    df.to_csv(csv_path, index=False)

    df['path'] = df['objectFilename'].apply(lambda x: direc_path / x)
    return df[['objectUrl', 'path']]


def download_single(url: str, path: Path | str) -> Path | None:
    '''
    Download a single file from its s3 url and write to a local file. Only downloads and writes if the file does not already exist.

    Args:
        url (str): URL to download file from
        path (Path): Path to write the file to

    Returns:
        Path | None: If a file is downloaded & written, returns the path to the file. Else returns `None`.
    '''
    if not isinstance(path, Path):
        path = Path(path)
    if not path.exists():
        resp = requests.get(url)
        if resp.status_code == requests.codes.ok:
            with open(path, 'wb') as file:
                file.write(resp.content)
                return path
        else:
            return None


def fetch_records(id: str,
                  json_out: str | Path,
                  key_name: str = 'NARA_KEY',
                  limit: int = 50) -> benedict:
    '''
    Prep and make request to the NARA API to get records for a given parent series ID, then write results to json file.  

    Args:
        id (str): Parent ID of series 
        json_out (str | Path): Path to write json file of results
        key_name (str): Name of the environment variable containing the NARA API key. Defaults "NARA_KEY".
        limit (int, optional): Number of record sets to return. Defaults to 50.

    Returns:
        benedict: Returns response JSON as a benedict object.
    '''
    base_url = 'https://catalog.archives.gov/api/v2/records/parentNaId'
    url = f'{base_url}/{id}'

    hdrs = {
        'Content-Type': 'application/json',
        'x-api-key': os.getenv(key_name)
    }
    prms = {'limit': limit}
    resp = requests.get(url, headers=hdrs, params=prms)
    # print(f'Request status: {resp.status_code}')
    resp.raise_for_status()
    # compare total record count to limit, print message if limit is lower
    resp_json = resp.json()
    json_bene = benedict(resp_json)
    total = int(json_bene[['body', 'hits', 'total', 'value']]) # type: ignore
    if total > limit:
        print(f'Warning: series {id} has {total} records, but limit is set to {limit}.')

    with open(json_out, 'w') as file:
        json.dump(resp_json, file)
        print(f'Results written to {json_out}')
    return json_bene


def get_args() -> tuple[str, int, bool]:
    '''
    Process command line arguments.  

    Returns:
        tuple[str, int, bool]: Returns a tuple of the parent series ID, limit for records query, and boolean indicating whether this is a dry run with no downloads.
    '''
    prsr = argparse.ArgumentParser(
        prog='NARA scraper',
        description='Download digitized NARA records based on parent series ID')
    prsr.add_argument('-i',
                      '--id',
                      type=str,
                      help='NARA parent series ID (default: %(default)s)',
                      default='5573655')
    prsr.add_argument('-l',
                      '--limit',
                      type=int,
                      help='Limit: total number of records to fetch (default: %(default)s)',
                      default=50)
    prsr.add_argument(
        '-n',
        '--no_download',
        help='Don\'t download files, just make request & write json',
        action='store_true')

    args = prsr.parse_args()
    return (args.id, args.limit, args.no_download)


def main() -> None:
    '''
    Main function body
    '''
    # load .env file for api key
    load_dotenv()

    # get base id from args
    base_id, limit, no_download = get_args()
    json_out = f'results_{base_id}.json'
    output_dir = f'output_{base_id}'

    # make api request for records, return benedict object, pluck out hits
    # in the process, write out results of api call to json
    results = fetch_records(id=base_id,
                            json_out=json_out,
                            key_name='NARA_KEY',
                            limit=limit)
    if no_download:
        print(f'Skipping download; see {json_out} for results\n')
        return
    records = results[['body', 'hits', 'hits']]

    # extract digitalObjects where they exist
    items = [recs_to_df(x) for x in records]
    # items is list of dicts, with meta = dict, objs = df--bind into single df
    prepped_items = [
        prep_output(output_dir, item) for item in items if item is not None
    ]
    n_items = len(prepped_items)
    # if df is empty, exit script
    if n_items == 0:
        print('No digitized records found.')
        return
    else:
        print(f'{n_items} digitized records found.')
    items_df = pd.concat(prepped_items)

    # download files in parallel with cute progress bar
    process_map(download_single,
                items_df['objectUrl'],
                items_df['path'],
                max_workers=num_cpu,
                chunksize=10)


if __name__ == '__main__':
    main()
