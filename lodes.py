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

import matplotlib.pyplot as plt
import fiona
import geopandas as gpd
from utils import *
from state_codes import State


###
def load_geoblocks(state, year='2021', cache=True):
    assert(len(year) == 4)
    year = str(year)

    if len(state) > 1:
        state = State.abb[state].lower()
    state_num = State.code[state.upper()]

    # Format the file url
    url_template = 'https://www2.census.gov/geo/tiger/TIGER{year}/TABBLOCK/tl_{year}_{fips}_tabblock10.zip'
    # url_template = 'https://www2.census.gov/geo/tiger/TGRGDB{yy}/tlgdb_{yyyy}_a_{state_num}_{state}.gdb.zip'
    file_url = url_template.format(**{'year': year, 'fips': state_num})

    # Fetch the file from URL or cache
    bytes_data = fetch_bytes(file_url, cache=cache)

    if '.gdb' in os.path.basename(file_url):
        # Read geodatabase into geopandas
        with fiona.io.ZipMemoryFile(bytes_data) as zip_memory_file:
            with zip_memory_file.open(os.path.basename(file_url).rstrip('.zip')) as collection:
                geodf = gpd.GeoDataFrame.from_features(collection)
    else:
        geodf = gpd.read_file(io.BytesIO(bytes_data))

    return geodf
