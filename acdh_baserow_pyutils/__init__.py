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
        r = requests.get(
            url=db_url, headers={"Authorization": f"JWT {self.br_jwt_token}"}
        )
        return r.json()

    def get_table_by_name(self, br_database_id, br_table_name):
        tables = self.list_tables(br_database_id)
        table_id = False
        for x in tables:
            if x["name"] == br_table_name:
                table_id = str(x["id"])
        return table_id

    def list_fields(self, br_table_id):
        url = f"{self.br_base_url}database/fields/table/{br_table_id}/"
        r = requests.get(url, headers={"Authorization": f"JWT {self.br_jwt_token}"})
        return r.json()

    def search_rows(self, br_table_id, q, query_field_id, lookup_type="contains"):
        url = f"{self.br_base_url}database/rows/table/{br_table_id}/?user_field_names=true&filter__field_{query_field_id}__{lookup_type}={q}"  # noqa
        r = requests.get(url, headers={"Authorization": f"JWT {self.br_jwt_token}"})
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

    def dump_tables_as_json(self, br_table_id, folder_name=None, indent=0):
        tables = self.list_tables(br_table_id)
        file_names = []
        for x in tables:
            data = {x["id"]: x for x in self.yield_rows(f"{x['id']}")}
            f_name = f"{x['name']}.json"
            if folder_name is not None:
                f_name = os.path.join(folder_name, f_name)
            with open(f_name, "w") as f:
                if indent:
                    json.dump(data, f, ensure_ascii=False, indent=indent)
                else:
                    json.dump(data, f, ensure_ascii=False)
            file_names.append(f_name)
        return file_names

    def fetch_table_field_dict(self, br_db_id):
        print(f"fetching table and field info for {br_db_id}")
        br_tables = self.list_tables(br_db_id)
        table_dict = {}
        for x in br_tables:
            field_dict = {}
            table_dict[x["name"]] = x
            for f in self.list_fields(x["id"]):
                field_dict[f["name"]] = f
            table_dict[x["name"]]["fields"] = field_dict
        br_table_dict = table_dict
        return br_table_dict

    def get_or_create(self, table_name, field_name, lookup_dict, q):
        br_table_id = lookup_dict[table_name]["id"]
        query_field_id = lookup_dict[table_name]["fields"][field_name]["id"]
        match = self.search_rows(br_table_id, q, query_field_id, lookup_type="equal")
        if match["count"] == 1:
            object, created = match["results"][0], False
        else:
            create_url = f"{self.br_base_url}database/rows/table/{br_table_id}/?user_field_names=true"
            item = {field_name: q}
            r = requests.post(
                create_url,
                headers={
                    "Authorization": f"Token {self.br_token}",
                    "Content-Type": "application/json",
                },
                json=item,
            )
            object, created = r.json(), True
        return object, created

    def __init__(
        self,
        br_user,
        br_pw,
        br_token,
        br_base_url="https://api.baserow.io/api/",
        br_db_id=None,
    ):
        self.br_user = br_user
        self.br_pw = br_pw
        self.br_token = br_token
        self.br_base_url = self.url_fixer(br_base_url)
        self.br_jwt_token = self.get_jwt_token()
        self.headers = {
            "Authorization": f"Token {self.br_token}",
            "Content-Type": "application/json",
        }
        if br_db_id:
            self.br_db_id = br_db_id
            self.br_table_dict = self.fetch_table_field_dict(self.br_db_id)

        else:
            self.br_db_id = None
            self.br_table_dict = None
