import json
import os
import requests

from acdh_id_reconciler import gnd_to_wikidata, geonames_to_wikidata
from AcdhArcheAssets.uri_norm_rules import get_normalized_uri


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

    def dump_tables_as_json(self, br_table_id, folder_name=None, indent=0):
        tables = self.list_tables(br_table_id)
        file_names = []
        for x in tables:
            data = {x['id']: x for x in self.yield_rows(f"{x['id']}")}
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

    def enrich_data(self, br_table_id, uri, field_name_input, field_name_update):
        table = [x for x in self.yield_rows(br_table_id=br_table_id)]
        br_rows_url = f"{self.br_base_url}database/rows/table/{br_table_id}/"
        v_wd = 0
        v_geo = 0
        for x in table:
            update = {}
            if uri == "gnd":
                if (len(x[field_name_input["gnd"]]) > 0):
                    norm_id = get_normalized_uri(x[field_name_input["gnd"]])
                    print(norm_id)
                    try:
                        wd = gnd_to_wikidata(norm_id)
                        wd = wd["wikidata"]
                        v_wd += 1
                        print(f"gnd id matched with wikidata: {wd}")
                    except Exception as err:
                        wd = "N/A"
                        print(err)
                        print(f"no match for {norm_id} found.")
                    update[field_name_update["wikidata"]] = wd
            if uri == "geonames":
                if (len(x[field_name_input["geonames"]]) > 0):
                    norm_id = get_normalized_uri(x[field_name_input["geonames"]])
                    print(norm_id)
                    try:
                        geo = geonames_to_wikidata(norm_id)
                        gnd = geo["gnd"]
                        wd = geo["wikidata"]
                        v_geo += 1
                        print(f"geonames id matched with gnd: {gnd} and wikidata: {wd}")
                    except Exception as err:
                        wd = "N/A"
                        gnd = "N/A"
                        print(err)
                        print(f"no match for {norm_id} found.")
                    update[field_name_update["gnd"]] = f"https://d-nb.info/gnd/{gnd}"
                    update[field_name_update["wikidata"]] = wd
            row_id = x["id"]
            url = f"{br_rows_url}{row_id}/?user_field_names=true"
            print(url)
            try:
                requests.patch(
                    url,
                    headers=self.headers,
                    json=update
                )
            except Exception as err:
                print(err)
        print(f"{v_wd} wikidata uri and {v_geo} geonames uri of {len(table)} table rows matched")

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
        self.headers = {"Authorization": f"Token {self.br_token}", "Content-Type": "application/json"}
