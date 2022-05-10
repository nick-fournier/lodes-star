import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import glob
import os
from io import BytesIO
from scipy.sparse import csr_matrix, coo_matrix
from pandas.api.types import CategoricalDtype

import sparse

class LODES:
    state, type, year = 'ca', 'od', '2019'
    filename = 'ca_od_aux_JT00_2019.csv.gz'

    def get_cache(self):
        cache = [f for f in glob.glob(os.path.join(os.getcwd(), '**/*.csv.gz'), recursive=True)]

    def get_latest_year(self, hrefs):
        years = [node.get('href').replace('.csv.gz', '')[-4:] for node in hrefs if node.get('href').endswith('.csv.gz')]
        return max(years)

    def lodes_to_sparse(self, df):
        geocodes = pd.unique(df[['w_geocode', 'h_geocode']].values.ravel('K'))
        jobs = [c for c in df.columns if c not in ['w_geocode', 'h_geocode']]
        shape = (len(geocodes), len(geocodes))

        # Map geocodes to coded index
        jobs_cat = CategoricalDtype(categories=sorted(jobs), ordered=True)
        geocodes_cat = CategoricalDtype(categories=sorted(geocodes), ordered=True)
        h_geocode_index = df['h_geocode'].astype(geocodes_cat).cat.codes
        w_geocode_index = df['w_geocode'].astype(geocodes_cat).cat.codes

        # Create dict of sparse matrices
        mat_dict = {k: coo_matrix((df[k], (w_geocode_index, h_geocode_index)), shape=shape) for k in jobs}

        # Stack into an n*m*k sparse coordinatate (COO) matrix
        mdmat = sparse.stack([sparse.COO(m) for m in mat_dict.values()], axis=-1)






    def fetch(self, state, type, year):
        base_url = 'https://lehd.ces.census.gov/data/lodes/LODES7'

        for type in ['od', 'rac', 'wac']:
            url = '/'.join([base_url, state.lower(), type.lower()])
            page = requests.get(url).text
            hrefs = BeautifulSoup(page, 'html.parser').find_all('a')

            if not year:
                year = self.get_latest_year(hrefs)
                print('No year specified, defaulting to latest year ' + year)

            flist = [node.get('href') for node in hrefs
                     if node.get('href').endswith('.csv.gz') and year in node.get('href')]


            data = {}
            for filename in flist:
                file_url = '/'.join([url, filename])
                r = requests.get(file_url)
                df = pd.read_csv(BytesIO(r.content), compression="gzip", dtype={'h_geocode': str, 'w_geocode': str})
                df.drop(columns='createdate', inplace=True)


if __name__ == "__main__":
    pass