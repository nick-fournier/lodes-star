from utils import *
import gzip
import pandas as pd


def load_lodes(state,
               zone_types='od',
               job_cat='JT00',
               year=None,
               cache=True):

    zone_types = [zone_types] if isinstance(zone_types, str) else zone_types
    job_cat = [job_cat] if isinstance(job_cat, str) else job_cat
    base_url = 'https://lehd.ces.census.gov/data/lodes/LODES7'

    if not year:
        year = get_latest_year(base_url, state)
        print('No year specified, defaulting to latest year ' + year)

    # Create file list
    flist = get_file_list(base_url, state, zone_types, job_cat, year)

    # Downloading files
    lodes = {}
    for fname, file_url in flist.items():
        key = fname.replace('.csv.gz', '')
        suffix = '{}/{}'.format(list(flist.keys()).index(fname) + 1, len(flist))

        # Fetch the file from URL or cache
        bytes_data = fetch_bytes(file_url, suffix, cache)

        # Decompress the gzip bytes data
        string_io = io.StringIO(gzip.decompress(bytes_data).decode('utf-8'))

        # read into pandas dataframe
        df = pd.read_csv(string_io, dtype={'h_geocode': str, 'w_geocode': str})

        lodes[key] = df.drop(columns='createdate')

    print('done')

    return lodes
