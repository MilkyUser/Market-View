import datetime
import json
import pandas as pd
from typing import List

config = json.load(open('config.json', 'r', encoding='utf8'))
data_sources = 'b3_data/COTAHIST_A{}.TXT'


class InvalidDatesException(Exception):
    def __init__(self, initial_date: datetime.date, final_date: datetime.date):
        self.initial_date = initial_date
        self.final_date = final_date
        self.invalid = False
        self.check_for()

    def check_for(self):
        if self.initial_date > self.final_date:
            super().__init__(f'{self.initial_date} should come before {self.final_date}.')
            self.invalid = True
        elif self.final_date > datetime.date.today():
            super().__init__(f'{self.final_date} is yet to come.')
            self.invalid = True


def download_data(
        initial_date: datetime.date,
        final_date: datetime.date
) -> None:

    import http
    import pathlib
    import requests
    import urllib3
    import zipfile
    from os import path, remove
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    def patch_http_response_read(func):
        def inner(*args):
            try:
                return func(*args)
            except http.client.IncompleteRead as e:
                return e.partial

        return inner

    http.client.HTTPResponse.read = patch_http_response_read(http.client.HTTPResponse.read)
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    session = requests.Session()
    retries = Retry(total=10, backoff_factor=1)
    session.mount('http://', HTTPAdapter(max_retries=retries))
    pathlib.Path(data_sources.split('/')[0]).mkdir(exist_ok=True)

    # Handles invalid dates set
    dates_exception = InvalidDatesException(initial_date, final_date)
    if dates_exception.invalid:
        raise dates_exception

    for year in range(initial_date.year, final_date.year + 1):

        # Skips file already downloaded
        if path.isfile(data_sources.format(year)):
            continue

        # Downloading zip file
        zip_path = f'b3_data/COTAHIST_A{year}.ZIP'
        with session.get(
                url=f'https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A{year}.ZIP',
                stream=True,
                verify=False,
        ) as download:

            # Writes data in a stream (so it doesn't eat all of your memory)
            with open(zip_path, 'wb') as f:
                for chunk in download.iter_content(chunk_size=8192):
                    f.write(chunk)

        # Unzipping file
        with zipfile.ZipFile(zip_path, 'r') as zip_file:
            zip_file.extractall('./b3_data')

        # Deleting zip file
        remove(zip_path)


def get_share_history(
        stock_names: List[str],
        initial_date: datetime.date,
        final_date: datetime.date
) -> List[pd.DataFrame]:

    download_data(initial_date, final_date)
    stock_names.sort()
    yearly_data = []

    for year in range(initial_date.year, final_date.year + 1):
        df = pd.read_fwf(
            data_sources.format(year),
            colspecs=config['colspec'],
            header=None,
            usecols=[1, 3, 12],
            names=['date', 'stock_name', 'stock_price']
        )
        filtered_rows = df[df['stock_name'].isin(stock_names)]
        filtered_rows = filtered_rows.pivot(
            index='date',
            columns='stock_name',
            values='stock_price',
        )
        filtered_rows.reset_index(level=0, inplace=True)
        filtered_rows = filtered_rows[
            (filtered_rows['date'] <= final_date.strftime('%Y%m%d')) &
            (filtered_rows['date'] >= initial_date.strftime('%Y%m%d'))
            ]
        yearly_data.append(filtered_rows)

    return yearly_data


def to_csv(
        stock_names: List[str],
        initial_date: datetime.date,
        final_date: datetime.date,
        file=None
):
    shares = get_share_history(stock_names, initial_date, final_date)
    shares = pd.concat(shares)
    print('YYYY-MM-DD', *[f'{elem};' for elem in shares.keys().tolist()][1:], sep=';', file=file)
    for index, row in shares.iterrows():
        row = row.tolist()
        print(f'{row[0][0:4]}-{row[0][4:6]}-{row[0][6:8]};', end='', file=file)
        print(*(elem for elem in row[1:]), sep=';', file=file)


if __name__ == '__main__':
    import sys

    to_csv(
        sys.argv[3:],
        datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d').date(),
        datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
    )
