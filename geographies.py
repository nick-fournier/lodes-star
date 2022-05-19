import gzip
import io
import geopandas
import zipfile
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
    url_template = 'https://www2.census.gov/geo/tiger/TGRGDB{yy}/tlgdb_{yyyy}_a_{state_num}_{state}.gdb.zip'
    file_url = url_template.format(**{'yy': year[2:], 'yyyy': year, 'state_num': state_num, 'state': state})

    # Fetch the file from URL or cache
    bytes_data = fetch_bytes(file_url, cache=cache)

    # TODO need to read gdb to geopandas?
    # Decompress the gzip bytes data
    with zipfile.ZipFile(io.BytesIO(bytes_data)) as zbuf:
        with zbuf.open(zbuf.namelist()[0]) as f:
            geodata = f.read()
            return f.read()



    # if ext == '.zip':
    #     with ZipFile('ca_od_aux_JT00_2002.zip') as zf:
    #         assert (len(zf.namelist()) == 1)
    #         with zf.open(zf.namelist()[0]) as f:
    #             return f.read()

    # read into geopandas


    print('done')

    return lodes