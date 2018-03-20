import os
import requests
from pprint import pprint
import json
import pandas as pd

class NoaaApi(object):
    '''
    Helper methods for working with NOAA's data API.
    See https://www.ncdc.noaa.gov/cdo-web/webservices/v2 for API documentation.
    '''

    def __init__(self, api_key):
        '''
        First, request an API token: https://www.ncdc.noaa.gov/cdo-web/token
        Second, add your token to your local `~/.bash_profile` or `~/.bash_rc`:
        `export NOAA_API_KEY='<your token here>'`
        Finally, we can access your API token.
        '''
        self.headers = {'token': api_key}
        self.endpoint = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?"
        self.payload = self._default_query_params()
        self.data = None

    def _update_payload(self, query_params):
        self.payload.update(**query_params)

    def _default_query_params(self):
        return {
        'datasetid': 'GSOM',
        'locationid': 'ZIP:80435',
        'startdate': pd.to_datetime('2013-10-06'),
        'enddate': pd.to_datetime('2016-11-11'),
        'limit': 1000,
        'offset': 0
        }

    def _make_request(self, query_params=None):
        if query_params is not None:
            self._update_payload(query_params)
        response = requests.get(self.endpoint, headers=self.headers, params=self.payload)
        print('request: {}'.format(response.url))
        print('status code: {}'.format(response.status_code))
        return response

    def _parse_response(self, response):
        try:
            list_of_dicts = response.json()['results']
            print('records returned: {n}'.format(n=len(list_of_dicts)))
            df = pd.DataFrame(list_of_dicts).set_index('station')
        except (json.JSONDecodeError, KeyError) as e:
            print('ERROR: {}'.format(e))
            df = pd.DataFrame()
        print(df)
        return df

    def _iterate_over_pages(self, query_params):
        df = pd.DataFrame()
        for i in range(100):
            self.payload.update({'offset': 1000*i})
            response = self._make_request(query_params)
            data_ = self._parse_response(response)
            df = df.append(data_)
            if self._iteration_complete(data_):
                df.to_csv('data/NOAA_complete_data.csv')
                self.data = df
                break
            else:
                data_.to_csv('data/NOAA_temp_data.csv')
                # self._write_to_temp_file(data_)
                continue

    def _write_to_temp_file(self, data, filename='temp_data', extension='txt'):
        assert isinstance(data, pd.DataFrame), 'expecting Pandas DataFrame'
        filepath = "data/NOAA_{}.{}".format(filename, extension)
        with open(filepath, 'a') as data_file:
            for index, row in data.iterrows():
                data_file.write(row.to_string())
        print("{n} records written to temp file".format(n=len(data)))

    def _iteration_complete(self, data_):
        n_records = len(data_)
        most_recent_date = data_.sort_values('date').iloc[-1].date.split('T')[0]
        print('number of records: {n}'.format(n=n_records))
        print('most recent record: {date}'.format(date=most_recent_date))
        if (n_records < 1000) or (most_recent_date == self.payload['enddate']):
            print('No more records to retrieve')
            return True
        else:
            return False

    def _iterate_over_years(self, query_params):
        year_range = pd.date_range(
        start=self.payload['startdate'], end=self.payload['enddate'], freq='12M'
        )
        for i, year in enumerate(year_range[:-1]):
            self.payload.update(
            {'startdate': year_range[i], 'enddate': year_range[i+1]}
            )
            self._iterate_over_pages(query_params)

    def get_data(self, query_params):
        if (self.payload['enddate'] - self.payload['startdate']).days >= 365:
            self._iterate_over_years(query_params)
        else:
            self._iterate_over_pages(query_params)

    def _valid_date_range(self, payload):
        pass



if __name__=="__main__":
    import os
    api_key = os.environ['NOAA_API_KEY']
    NOAA = NoaaApi(api_key)
    payload = {
        'datasetid': 'GSOM',
        'locationid': 'ZIP:80435',
        'startdate': '2010-01-01',
        'enddate': '2015-01-01'
        }
    NOAA.get_data(payload)
