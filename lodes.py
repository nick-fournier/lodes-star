import requests
import pandas as pd
import glob
import os
import shutil
from tqdm.auto import tqdm
from bs4 import BeautifulSoup


###
def get_cache_list(cache_dir=os.getcwd(), full_path=True):
    if full_path:
        return [f for f in glob.glob(os.path.join(cache_dir, '**/*.csv.gz'), recursive=True)]
    else:
        return [os.path.basename(f) for f in glob.glob(os.path.join(cache_dir, '**/*.csv.gz'), recursive=True)]

def get_latest_year(base_url, state):
    common_years = None
    for zone in ['od', 'rac', 'wac']:
        url = '/'.join([base_url, state.lower(), zone.lower()])
        page = requests.get(url).text
        hrefs = [x.get('href') for x in BeautifulSoup(page, 'html.parser').find_all('a')]
        years = [node.replace('.csv.gz', '')[-4:] for node in hrefs if node.endswith('.csv.gz')]

        if not common_years:
            common_years = set(years)
        else:
            common_years = common_years.intersection(years)

    return max(common_years)

def file_fetch(file_url, of_string="", fetch_only=True):
    file_name = file_url.split('/')[-1]
    file_path = os.path.join('cache', file_name)

    # Check if already cached
    if not os.path.exists(file_path):
        desc_lab = ' '.join(['Fetching', file_name, of_string])
        # make an HTTP request within a context manager
        with requests.get(file_url, stream=True) as r:
            # check header to get content length, in bytes
            total_length = int(r.headers.get("Content-Length"))
            # implement progress bar via tqdm
            with tqdm.wrapattr(r.raw, "read", total=total_length, desc=desc_lab) as raw:
                # save the output to a file
                with open(file_path, 'wb') as output:
                    shutil.copyfileobj(raw, output)

    # Read the data into pandas
    if not fetch_only:
        print(' '.join(['Loading cached', file_name, of_string]))
        df = pd.read_csv(file_path, compression="gzip", dtype={'h_geocode': str, 'w_geocode': str})
        df.drop(columns='createdate', inplace=True)
        return df
    return

def load_lodes(state, zone_types=None, year=None, fetch_only=False):
    zone_types = ['od', 'rac', 'wac'] if not zone_types else zone_types
    zone_types = [zone_types] if isinstance(zone_types, str) else zone_types
    base_url = 'https://lehd.ces.census.gov/data/lodes/LODES7'

    if not os.path.exists('cache'):
        os.mkdir('cache')

    if not year:
        print('No year specified', end=' ')
        year = get_latest_year(base_url, state)
        print(', defaulting to latest year ' + year)

    for zone in zone_types:
        url = '/'.join([base_url, state.lower(), zone.lower()])
        page = requests.get(url).text
        hrefs = [x.get('href') for x in BeautifulSoup(page, 'html.parser').find_all('a')]
        flist = [node for node in hrefs if node.endswith('.csv.gz') and year in node]

        # Downloading
        lodes = {}
        for fname in flist:
            key = os.path.splitext(fname)[0]
            of_string = '/'.join([str(flist.index(fname) + 1), str(len(flist))])
            file_url = os.path.join(url, fname)
            lodes[key] = file_fetch(file_url, of_string, fetch_only)
        print('done')

    return lodes


if __name__ == "__main__":
    tmp = load_lodes(state='ca', zone_types='wac', year='2019', fetch_only=True)

