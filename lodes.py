from utils import *
import pandas as pd


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
        os.mkdir('cache/lodes')

    if not year:
        print('No year specified', end=' ')
        year = get_latest_year(base_url, state)
        print(', defaulting to latest year ' + year)

    lodes = {}
    for zone in zone_types:
        if not os.path.exists('cache'):
            os.mkdir(os.path.join('cache/lodes', zone))

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

            data_string = file_fetch(file_url, of_string, cache)
            if cache_only:
                print(' '.join(['Skipping cached', fname, of_string, "while fetch_only=True"]))
                return
            df = pd.read_csv(io.StringIO(data_string), dtype={'h_geocode': str, 'w_geocode': str})
            lodes[key] = df.drop(columns='createdate')

    print('done')

    return lodes
