import requests
import io
import os
import glob
import shutil
import gzip
from zipfile import ZipFile
from bs4 import BeautifulSoup
from tqdm.auto import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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


def stream_to_file(response, file_path, desc_lab=""):
    # check header to get content length, in bytes
    total_length = int(response.headers.get("Content-Length"))
    # implement progress bar via tqdm
    with tqdm.wrapattr(response.raw, "read", total=total_length, desc=desc_lab) as raw:
        # save the output to a file
        with open(file_path, 'wb') as output:
            shutil.copyfileobj(raw, output)


def file_fetch(file_url, of_string="", cache=True):
    file_name = file_url.split('/')[-1]
    file_path = os.path.join('cache', file_name)

    # Check if already cached
    if not os.path.exists(file_path):
        # make an HTTP request within a context manager
        s = requests.Session()
        response = requests_retry_session(session=s).get(file_url, stream=True, timeout=5)

        if cache:
            obj = open(file_path, 'wb')
        else:
            obj = open(os.devnull, 'wb')

        bytes_data = b''
        with tqdm.wrapattr(obj, "write",
                           desc=' '.join(['Fetching', file_name, of_string]),
                           total=int(response.headers.get("Content-Length"))
                           ) as out:

            # save the output to a file
            for buf in response.iter_content(io.DEFAULT_BUFFER_SIZE):
                out.write(buf)
                bytes_data += buf

        return gzip.decompress(bytes_data).decode('utf-8')

    # Read existing data
    else:
        print(' '.join(['Loading cached', file_name, of_string]))
        # open text file in read mode
        ext = os.path.splitext(file_path)[-1]

        if ext == '.gz':
            with gzip.open(file_path, "r") as gzf:
                return gzf.read()

        if ext == '.zip':
            with ZipFile('ca_od_aux_JT00_2002.zip') as zf:
                assert(len(zf.namelist()) == 1)
                with zf.open(zf.namelist()[0]) as f:
                    return f.read()
