import os
import json
import pymongo
import requests
import pandas as pd
from pprint import pprint

# import pymongo_helpers

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
        # self._init_mongo_client()

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
        return response

    # def _parse_response(self, response):
    #     try:
    #         list_of_dicts = response.json()['results']
    #         print('records returned: {n}'.format(n=len(list_of_dicts)))
    #         df = pd.DataFrame(list_of_dicts).set_index('station')
    #     except (json.JSONDecodeError, KeyError) as e:
    #         print('ERROR: {}'.format(e))
    #         df = pd.DataFrame()
    #     print(df)
    #     return df

    def _valid_response(self, response):
        if response.status_code == 200:
            if 'results' in response.json().keys():
                print('SUCCESS: valid response')
                return True
            elif self._is_empty(json):
                print('WARNING: empty response')
                return False
            else:
                print('WARNING: unexpected response')
                return False
        else:
            print ('WARNING: request failed with status code {}'.format(response.status_code))

    def _parse_response(self, response):
        return response.json()['results']

    def _convert_to_df(self, list_of_dicts):
        df = pd.DataFrame(list_of_dicts).set_index('station')
        return df

    def _is_empty(any_structure):
        if any_structure:
            return False
        else:
            print('WARNING: Data structure is empty.')
            return True

    def _iteration_complete(self, data_):
        df = self._convert_to_df(data_)
        n_records = len(df)
        most_recent_date = df.sort_values('date').iloc[-1].date.split('T')[0]
        print('number of records: {n}'.format(n=n_records))
        print('most recent record: {date}'.format(date=most_recent_date))
        if (n_records < 1000) or (most_recent_date == self.payload['enddate']):
            print('No more records to retrieve')
            return True
        else:
            return False

    def _iterate_over_pages(self, query_params):
        for i in range(100):
            self.payload.update({'offset': 1000*i})
            response = self._make_request(query_params)
            if self._valid_response(response):
                data_ = self._parse_response(response)
                self._insert_documents_into_db(data_)
                if self._iteration_complete(data_):
                    break
                else:
                    continue
            else:
                continue

    # def _write_to_temp_file(self, data, filename='temp_data', extension='txt'):
    #     assert isinstance(data, pd.DataFrame), 'expecting Pandas DataFrame'
    #     filepath = "data/NOAA_{}.{}".format(filename, extension)
    #     with open(filepath, 'a') as data_file:
    #         for index, row in data.iterrows():
    #             data_file.write(row.to_string())
    #     print("{n} records written to temp file".format(n=len(data)))

    def _iterate_over_years(self, query_params):
        year_range = pd.date_range(
        start=self.payload['startdate'],
        end=self.payload['enddate'],
        freq='12M'
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

    def _insert_documents_into_db(self, documents):
        collection = self._init_mongo_client()
        print('{} documents received'.format(len(documents)))
        print('inserting documents into mongodb...')
        document_count = 0
        for doc in documents:
            try:
                collection.insert(doc)
                document_count += 1
            except pymongo.errors.DuplicateKeyError:
                print("duplicate record found... skipping...")
                continue
        print('done. {} documents successfully inserted to MongoDB'.format(document_count))

    def _init_mongo_client(self):
        client = pymongo.MongoClient()  # Initiate Mongo client
        db = client.NOAA            # Access database
        coll = db.data          # Access collection
        return db.data      # return collection pointer



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
