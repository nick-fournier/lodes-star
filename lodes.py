import requests
import pandas as pd
import glob
import os
import shutil
import sparse
from tqdm.auto import tqdm
from bs4 import BeautifulSoup
from io import BytesIO
from scipy.sparse import coo_matrix
from pandas.api.types import CategoricalDtype


class SparseLodes:
    def df_to_sparse(self, df):
        geocodes = pd.unique(df[['w_geocode', 'h_geocode']].values.ravel('K'))
        jobs = [c for c in df.columns if c not in ['w_geocode', 'h_geocode']]
        shape = (len(geocodes), len(geocodes))

        # Map geocodes to coded index
        self.jobs_cat = CategoricalDtype(categories=sorted(jobs), ordered=True)
        self.geocodes_cat = CategoricalDtype(categories=sorted(geocodes), ordered=True)
        self.h_geocode_index = df['h_geocode'].astype(self.geocodes_cat).cat.codes
        self.w_geocode_index = df['w_geocode'].astype(self.geocodes_cat).cat.codes

        # Create dict of sparse matrices
        mat_dict = {k: coo_matrix((df[k], (self.w_geocode_index, self.h_geocode_index)), shape=shape) for k in jobs}

        # Stack into an n*n*k sparse coordinatate (COO) matrix
        # where n is the number of zones and k are the strata
        self.sparse_mat = sparse.stack([sparse.COO(m) for m in mat_dict.values()], axis=-1)


###
def get_cache():
    cache = [f for f in glob.glob(os.path.join(os.getcwd(), '**/*.csv.gz'), recursive=True)]

def get_latest_year(hrefs):
    years = [node.get('href').replace('.csv.gz', '')[-4:] for node in hrefs if node.get('href').endswith('.csv.gz')]
    return max(years)

def file_fetch(url, filename):
    print('Fetching ' + filename)
    # make an HTTP request within a context manager
    with requests.get('/'.join([url, filename]), stream=True) as r:
        # check header to get content length, in bytes
        total_length = int(r.headers.get("Content-Length"))
        # implement progress bar via tqdm
        with tqdm.wrapattr(r.raw, "read", total=total_length, desc="") as raw:
            # save the output to a file
            with open(os.path.join('cache', filename), 'wb') as output:
                shutil.copyfileobj(raw, output)

            df = pd.read_csv(BytesIO(r.content), compression="gzip", dtype={'h_geocode': str, 'w_geocode': str})
            df.drop(columns='createdate', inplace=True)

    return df

def lode_fetch(state, year):
    state, year = 'ca', '2019'
    zone = 'od'
    filename = 'ca_od_main_JT00_2019.csv.gz'
    base_url = 'https://lehd.ces.census.gov/data/lodes/LODES7'

    if not os.path.exists('cache'):
        os.mkdir('cache')

    # if not year:
    #     year = get_latest_year(hrefs)
    #     print('No year specified, defaulting to latest year ' + year)

    for zone in ['od', 'rac', 'wac']:
        url = '/'.join([base_url, state.lower(), zone.lower()])
        page = requests.get(url).text
        hrefs = BeautifulSoup(page, 'html.parser').find_all('a')

        flist = [node.get('href') for node in hrefs
                 if node.get('href').endswith('.csv.gz') and year in node.get('href')]

        tmp = file_fetch(url, filename)

        tmp = {os.path.splitext(fname)[0]: file_fetch(url, fname) for fname in flist}


if __name__ == "__main__":
    pass