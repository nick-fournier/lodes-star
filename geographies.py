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
    geodf = gpd.read_file('cache/geo/tiger/TIGER2019/TABBLOCK/tl_2019_25_tabblock10.zip')

    # # Read geodatabase into geopandas
    # with fiona.io.ZipMemoryFile(bytes_data) as zip_memory_file:
    #     with zip_memory_file.open(os.path.basename(file_url).rstrip('.zip')) as collection:
    #         geodf = gpd.GeoDataFrame.from_features(collection)

    return geodf
