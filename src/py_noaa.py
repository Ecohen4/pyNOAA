import json
import pymongo
import requests
import pandas as pd
from datetime import datetime
from pprint import pprint


class NoaaApi(object):
    '''
    Helper methods for working with NOAA's data API.
    See https://www.ncdc.noaa.gov/cdo-web/webservices/v2 for API documentation.
    '''
    def __init__(self, api_key, query_params=None):
        '''
        First, request an API token: https://www.ncdc.noaa.gov/cdo-web/token
        Second, add your token to your local `~/.bash_profile` or `~/.bash_rc`:
        `export NOAA_API_KEY='<your token here>'`
        Finally, we can access your API token.
        '''
        self.headers = {'token': api_key}
        self.endpoint = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data?"
        self._set_payload(query_params)
        self._init_mongo_client()

    def get_data(self, query_params=None):
        if query_params is not None:
            self._update_payload(query_params)
            self._init_mongo_client()
        if (self.payload['enddate'] - self.payload['startdate']).days >= 365:
            self._iterate_over_years()
        else:
            self._iterate_over_pages()

    def _iterate_over_years(self):
        year_range = pd.date_range(
            start=self.payload['startdate'],
            end=self.payload['enddate'],
            freq='12M'
        )
        for i, year in enumerate(year_range[:-1]):
            self.payload.update(
            {'startdate': year_range[i].date(),
            'enddate': year_range[i+1].date()
            })
            print('------------------------------')
            print('Requesting data from {} to {}'.format(
            self.payload['startdate'], self.payload['enddate'])
            )
            print('------------------------------')
            self._iterate_over_pages()

    def _iterate_over_pages(self, max_pages=100):
        exception_count = 0
        for i in range(max_pages):
            print('requesting page {}...'.format(i))
            self.payload.update({'offset': 1000*i})
            response = self._make_request()
            if self._valid_response(response):
                data_ = self._parse_response(response)
                self._insert_documents_into_db(data_)
                if self._iteration_complete(data_):
                    break
                else:
                    continue
            else:
                continue
                exception_count += 1
                if exception_count > 20:
                    print('Too many excpetions... exiting')
                    break

    def _make_request(self):
        response = requests.get(self.endpoint, headers=self.headers, params=self.payload)
        return response

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
            print(response.url)

    def _parse_response(self, response):
        return response.json()['results']

    def _iteration_complete(self, data_):
        df = self._convert_to_df(data_)
        n_records = len(df)
        oldest_date = df.sort_values('date').iloc[0].date.split('T')[0]
        most_recent_date = df.sort_values('date').iloc[-1].date.split('T')[0]
        print('number of records: {n}'.format(n=n_records))
        print('oldest record: {date}'.format(date=oldest_date))
        print('most recent record: {date}'.format(date=most_recent_date))
        if (most_recent_date == self.payload['enddate']):
            print('Reached end of chunked date range')
            return True
        elif (n_records < 1000):
            print('No more records to retrieve')
            return True
        else:
            return False

    def _set_payload(self, query_params=None):
        if query_params is None:
            query_params = self._default_query_params()
            print('WARNING: No query parameters provided... falling back on arbitrary defaults')
        self.payload = query_params
        self._convert_datetime_to_date_only()
        print('query parameters set:')
        pprint(self.payload)

    def _update_payload(self, query_params):
        self.payload.update(**query_params)
        self._convert_datetime_to_date_only()

    def _default_query_params(self):
        return {
        'datasetid': 'GSOM',
        'locationid': 'ZIP:80435',
        'startdate': pd.to_datetime('2013-10-06').date(),
        'enddate': pd.to_datetime('2016-11-11').date(),
        'units': 'metric',
        'limit': 1000,
        'offset': 0
        }

    def _convert_datetime_to_date_only(self):
        self.payload.update({
        'startdate': pd.to_datetime(self.payload['startdate']).date(),
        'enddate': pd.to_datetime(self.payload['enddate']).date()
        })

    def _convert_to_df(self, list_of_dicts):
        df = pd.DataFrame(list_of_dicts).set_index('station')
        return df

    def _is_empty(any_structure):
        if any_structure:
            return False
        else:
            print('WARNING: Data structure is empty.')
            return True

    def _insert_documents_into_db(self, documents):
        print('{} documents received'.format(len(documents)))
        print('inserting documents into mongodb...')
        document_count = 0
        for doc in documents:
            try:
                self.collection.insert(doc)
                document_count += 1
            except pymongo.errors.DuplicateKeyError:
                print("duplicate record found... skipping...")
                continue
        print('done. {} documents successfully inserted to MongoDB'.format(document_count))

    def _init_mongo_client(self):
        collection_name = self.payload['datasetid']
        client = pymongo.MongoClient()   # Initiate Mongo client
        db = client.NOAA                 # Access database
        collection = db[collection_name] # Access collection
        self.collection = collection     # assign pointer to class attribute

    def _debug_statement(self):
        print("why am I broken?")

    def _query_collection(query):
        return self.collection.find(query)

    def _convert_date_string_to_datetime_object(self,
            attr="date",
            date_format="%Y-%m-%d %H:%M:%S.%f"
            ):
        '''
        convert date strings to ISO datetime objects.
        attr is the column name which needs to be modified
        date_format is the format of the string eg : "%Y-%m-%d %H:%M:%S.%f"
        '''
        for obj in self.collection.find():
            if obj[attr]:
                if type(obj[attr]) is not datetime:
                    time = datetime.strptime(obj[attr], date_format)
                    col.update({'_id':obj['_id']},{'$set':{attr : time}})


if __name__=="__main__":
    import os
    api_key = os.environ['NOAA_API_KEY']
    payload = {
    'datasetid': 'GHCND',
    'locationid': 'FIPS:08117',
    'startdate': pd.to_datetime('2012-12-31').date(),
    'enddate': pd.to_datetime('2017-12-31').date(),
    'units': 'metric',
    'limit': 1000
    }
    NOAA = NoaaApi(api_key, payload)
    NOAA.get_data(payload)
