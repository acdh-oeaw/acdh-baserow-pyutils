import json
import os
import requests


class BaseRowClient:
    def get_jwt_token(self):
        url = f"{self.br_base_url}user/token-auth/"
        payload = {"password": self.br_pw, "username": self.br_user}
        r = requests.post(url=url, json=payload)
        return r.json()["token"]

    def url_fixer(self, url):
        if url.endswith("/"):
            return url
        else:
            return f"{url}/"

    def list_tables(self, br_database_id):
        db_url = f"{self.br_base_url}database/tables/database/{br_database_id}/"
        r = requests.get(url=db_url, headers={'Authorization': f'JWT {self.br_jwt_token}'})
        return r.json()

    def yield_rows(self, br_table_id, filters={}):
        br_rows_url = f"{self.br_base_url}database/rows/table/{br_table_id}/"
        url = f"{br_rows_url}?user_field_names=true"
        if filters:
            for key, value in filters.items():
                url += f"&{key}={value}"
        next_page = True
        while next_page:
            print(url)
            response = None
            result = None
            x = None
            response = requests.get(url, headers=self.headers)
            result = response.json()
            next_page = result["next"]
            url = result["next"]
            for x in result["results"]:
                yield x

    def dump_tables_as_json(self, br_table_id, folder_name=None):
        tables = self.list_tables(br_table_id)
        file_names = []
        for x in tables:
            data = [x for x in self.yield_rows(f"{x['id']}")]
            f_name = f"{x['name']}.json"
            if folder_name is not None:
                f_name = os.path.join(folder_name, f_name)
            with open(f_name, "w") as f:
                json.dump(data, f, ensure_ascii=False)
            file_names.append(f_name)
        return file_names

    def __init__(
        self,
        br_user,
        br_pw,
        br_token,
        br_base_url="https://api.baserow.io/api/",
    ):
        self.br_user = br_user
        self.br_pw = br_pw
        self.br_token = br_token
        self.br_base_url = self.url_fixer(br_base_url)
        self.br_jwt_token = self.get_jwt_token()
        self.headers = {"Authorization": f"Token {self.br_token}"}
