import os
import requests

from .utils import NoTokenFound


class BaseRowClient():

    def yield_rows(self, table_id, token, filters={}):
        url = f"{self.baserow_url}{table_id}/?user_field_names=true"
        if filters:
            for key, value in filters.items():
                url += f"&{key}={value}"
        next_page = True
        while next_page:
            print(url)
            response = None
            result = None
            x = None
            response = requests.get(
                url,
                headers={
                    "Authorization": f"Token {token}"
                }
            )
            result = response.json()
            next_page = result['next']
            url = result['next']
            for x in result['results']:
                yield x
    
    def __init__(
        self,
        baserow_url="https://baserow.acdh-dev.oeaw.ac.at/api/database/rows/table/",
        baserow_token=None
    ):

        if baserow_token is None:
            self.baserow_token = os.environ.get('BASEROW_TOKEN', None)
        else:
            self.baserow_token = baserow_token
        if self.baserow_token is None:
            raise NoTokenFound

        if baserow_url.endswith('/'):
            self.baserow_url = baserow_url
        else:
            self.baserow_url = f"{baserow_url}/"
