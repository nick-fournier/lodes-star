import pandas as pd
import shutil
from utils import *
from tqdm.auto import tqdm


###
def file_fetch(file_url, of_string="", cache=True, cache_only=True):
    file_name = file_url.split('/')[-1]
    file_path = os.path.join('cache', file_name)

    # Check if already cached
    if not os.path.exists(file_path):
        desc_lab = ' '.join(['Fetching', file_name, of_string])
        # make an HTTP request within a context manager
        s = requests.Session()
        response = requests_retry_session(session=s).get(file_url, stream=True, timeout=5)

        if cache:
            with response as r:
                # check header to get content length, in bytes
                total_length = int(r.headers.get("Content-Length"))
                # implement progress bar via tqdm
                with tqdm.wrapattr(r.raw, "read", total=total_length, desc=desc_lab) as raw:
                    # save the output to a file
                    with open(file_path, 'wb') as output:
                        shutil.copyfileobj(raw, output)
            # Read the data into pandas
            if not cache_only:
                print(' '.join(['Loading cached', file_name, of_string]))
                df = pd.read_csv(file_path, compression="gzip", dtype={'h_geocode': str, 'w_geocode': str})
                df.drop(columns='createdate', inplace=True)
                return df
            else:
                print(' '.join(['Skipping cached', file_name, of_string, "while fetch_only=True"]))
                return
        else:
            return pd.read_csv(iterable_to_stream(response.iter_content()), sep=',', compression='gzip')


def load_lodes(state,
               zone_types='od',
               job_cat='JT00',
               year=None,
               cache=True,
               cache_only=False):

    zone_types = [zone_types] if isinstance(zone_types, str) else zone_types
    job_cat = [job_cat] if isinstance(job_cat, str) else job_cat
    base_url = 'https://lehd.ces.census.gov/data/lodes/LODES7'

    if not os.path.exists('cache'):
        os.mkdir('cache')

    if not year:
        print('No year specified', end=' ')
        year = get_latest_year(base_url, state)
        print(', defaulting to latest year ' + year)

    lodes = {}
    for zone in zone_types:
        url = '/'.join([base_url, state.lower(), zone.lower()])
        s = requests.Session()
        response = requests_retry_session(session=s).get(url, stream=True, timeout=5)
        hrefs = [x.get('href') for x in BeautifulSoup(response.text, 'html.parser').find_all('a')]
        flist = [node for node in hrefs
                 if node.endswith('.csv.gz')
                 and year in node
                 and any([True for x in job_cat if x in node])]

        # Downloading
        for fname in flist:
            key = os.path.splitext(fname)[0]
            of_string = '/'.join([str(flist.index(fname) + 1), str(len(flist))])
            file_url = os.path.join(url, fname)
            lodes[key] = file_fetch(file_url, of_string, cache, cache_only)
    print('done')

    return lodes
