import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import io
import os
import glob
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

        s = requests.Session()
        response = requests_retry_session(session=s).get(url, timeout=5)

        hrefs = [x.get('href') for x in BeautifulSoup(response.text, 'html.parser').find_all('a')]
        years = [node.replace('.csv.gz', '')[-4:] for node in hrefs if node.endswith('.csv.gz')]

        if not common_years:
            common_years = set(years)
        else:
            common_years = common_years.intersection(years)

    return max(common_years)


def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def iterable_to_stream(iterable, buffer_size=io.DEFAULT_BUFFER_SIZE):
    """
    Lets you use an iterable (e.g. a generator) that yields bytestrings as a read-only
    input stream.

    The stream implements Python 3's newer I/O API (available in Python 2's io module).
    For efficiency, the stream is buffered.
    """
    class IterStream(io.RawIOBase):
        def __init__(self):
            self.leftover = None

        def readable(self):
            return True

        def readinto(self, b):
            try:
                l = len(b)  # We're supposed to return at most this much
                chunk = self.leftover or next(iterable)
                output, self.leftover = chunk[:l], chunk[l:]
                b[:len(output)] = output
                return len(output)
            except StopIteration:
                return 0    # indicate EOF
    return io.BufferedReader(IterStream(), buffer_size=buffer_size)
