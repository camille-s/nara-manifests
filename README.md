# National Archives batch download

This is a project to batch download files from the National Archives & Records Administration (NARA) based on a series ID. 

## Steps to run

1. Set up & activate a Python environment. Python dependencies are managed with conda / mamba; install dependencies by creating an environment based on the file `environment.yml`:
2. Get a [NARA API key](https://catalog.archives.gov/api/v2/api-docs/) and save it in a file `.env`.
3. Run the python script. It takes 3 arguments, all optional:

``` bash
python fetch_records.py --help
usage: NARA scraper [-h] [-i ID] [-l LIMIT] [-n]

Download digitized NARA records based on parent series ID

options:
  -h, --help         show this help message and exit
  -i, --id ID        NARA parent series ID (default: 5573655)
  -l, --limit LIMIT  Limit: total number of records to fetch (default: 50)
  -n, --no_download  Don't download files, just make request & write json
```

The default ID points to a series I was originally asked to scrape.

4. Optionally, you can process multiple series at once with a bash script. Put comma delimited names and IDs of interest into a text file (current working one is `parent_ids.txt`), then call the bash script. It takes up to 3 positional arguments, mirroring the ones for the python script: the path to the file of IDs, the limit, and optionally the text `"dryrun"` to trigger the python script's `--no_download` flag.
5. First the script makes a query to the API and dumps the results into a json file titled like `results_{series-id}.json`. Then (unless it's in dry-run mode), it creates a folder `output_{series-id}`. Within that, each record that has digital objects available gets a folder like `{record-id}_{record-title}`. Within that folder will be all the record's digital objects (mostly JPGs, sometimes PDFs), plus a CSV file of metadata for each object. This metadata includes the AWS S3 bucket URL from which each file was downloaded, for easy access in the future.

The python script runs in parallel, so downloading a series with tens of thousands of images can be quite fast (running on a 16-core laptop I got ~21k images downloaded in under 10 minutes). However, you need a lot of file space: my current collection of 6 series is 42GB. As such, PDFs and images are kept out of git tracking.

## Why does this project exist?

Because the US is doing its best to go full fascist dystopia. That includes [purging data](https://www.404media.co/archivists-work-to-identify-and-save-the-thousands-of-datasets-disappearing-from-data-gov/) and whitewashing history. Us nerds need to step up.

Anyone erasing the history of america's sins can get wrecked.