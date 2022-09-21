import os
import requests

from .utils import BaseRowUtilsError


class BaseRowClient():

    def url_fixer(self, url):
        if url.endswith('/'):
            return url
        else:
            return f"{url}/"

    def yield_rows(self, table_id, token, filters={}):
        url = f"{self.br_url}{table_id}/?user_field_names=true"
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
        br_table_id=None,
        br_base_url="https://api.baserow.io/api/",
        br_token=None
    ):
        if br_table_id is None:
            raise BaseRowUtilsError(msg="No Table-ID is set")
        else:
            self.br_table_id = br_table_id

        if br_token is None:
            self.br_token = os.environ.get('br_TOKEN', 'NOT_SET')
        else:
            self.br_token = br_token
        if self.br_token is None or self.br_token == 'NOT_SET':
            raise BaseRowUtilsError

        self.br_base_url = self.url_fixer(br_base_url)
        self.br_rows_url = f"{self.br_base_url}database/rows/table/{self.br_table_id}/"
