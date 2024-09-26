from typing import Union
import json
import os
import requests


def get_related_table_info(
    table_name: str, field_name: str, table_field_dict: dict
) -> tuple:
    """returns the name and the id of the related table

    Args:
        table_name (str): the name of current table e.g. "courses"
        field_name (str): the name of the current field e.g. "university"
        table_field_dict (dict): a dict providing information of the tables and fields of the database\
        as returned by `br_client.fetch_table_field_dict(BASEROW_DB_ID)`

    Returns:
        tuple: returns the ID and the name of the related table
    """
    field_dict = table_field_dict[table_name]["fields"][field_name]
    related_table_id = field_dict["link_row_table_id"]
    for _, value in table_field_dict.items():
        if value["id"] == related_table_id:
            related_table_name = value["name"]
            break
    return related_table_id, related_table_name


class BaseRowClient:
    def get_jwt_token(self) -> str:
        """fetches a baserow auth token

        Returns:
            str: the baserow auth token
        """
        url = f"{self.br_base_url}user/token-auth/"
        payload = {"password": self.br_pw, "username": self.br_user}
        r = requests.post(url=url, json=payload)
        return r.json()["token"]

    def url_fixer(self, url: str) -> str:
        """checks if the passed in URL ends with slash and appends one if not

        Args:
            url (str): URL to check (or any other string)

        Returns:
            str: URL ending with "/"
        """
        if url.endswith("/"):
            return url
        else:
            return f"{url}/"

    def list_tables(self, br_database_id: Union[int, str]) -> list:
        """retuns the baserow api enspoint listing all tables of the given database

        Args:
            br_database_id (Union[int, str]): The ID of the database

        Returns:
            list: a list with dicts like `{'id': 100947, 'name': 'place', 'order': 2, 'database_id': 41426}`
        """
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
            with open(f_name, "w", encoding="utf-8") as f:
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

    def delete_table(self, table_id):
        url = f"{self.br_base_url}database/tables/{table_id}/"
        r = requests.delete(
            url,
            headers={
                "Authorization": f"JWT {self.br_jwt_token}",
                "Content-Type": "application/json",
            },
        )
        if r.status_code == 204:
            object, deleted = {"status": f"table {table_id} deleted"}, True
        else:
            object, deleted = {"error": r.status_code}, False
        return object, deleted

    def create_table(self, table_name, fields=None):
        database_id = self.br_db_id
        url = f"{self.br_base_url}database/tables/database/{database_id}/"
        payload = {"name": table_name}
        if fields is not None:
            payload["data"] = fields
            payload["first_row_header"] = True
        r = requests.post(
            url=url,
            headers={
                "Authorization": f"JWT {self.br_jwt_token}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        if r.status_code == 200:
            object, created = r.json(), True
        else:
            object, created = {"error": r.status_code}, False
        return object, created

    def delete_table_fields(self, br_table_id, field_names):
        object, deleted = {"status": "no fields to delete"}, True
        for f in self.list_fields(br_table_id):
            if f["name"] in field_names:
                print("Deleting field... ", f["name"], f["id"])
                url = f"{self.br_base_url}database/fields/{f['id']}/"
                r = requests.delete(
                    url,
                    headers={
                        "Authorization": f"JWT {self.br_jwt_token}",
                        "Content-Type": "application/json",
                    },
                )
                if r.status_code == 200:
                    print(
                        f"Deleted field {f['name']} with id: {f['id']} in {br_table_id}"
                    )
                    object, deleted = r.json(), True
                else:
                    print(f"Error {r.status_code} with {br_table_id} in delete_fields")
                    object, deleted = {"error": r.status_code}, False
        return object, deleted

    def create_table_fields(self, br_table_id, br_table_fields):
        url = f"{self.br_base_url}database/fields/table/{br_table_id}/"
        payload, valid = self.validate_table_fields_type(br_table_fields)
        if valid:
            for field in payload:
                r = requests.post(
                    url=url,
                    headers={
                        "Authorization": f"JWT {self.br_jwt_token}",
                        "Content-Type": "application/json",
                    },
                    json=field,
                )
                if r.status_code == 200:
                    object, created = r.json(), True
                else:
                    object, created = {"error": r.status_code}, False
        else:
            object, created = {"error": "Field type schema wrong."}, valid
            print(
                object["error"],
                "Visit https://api.baserow.io/api/redoc/ to learn more.",
            )
        return object, created

    def validate_table_fields_type(self, br_table_fields):
        valid = True
        required_keys = ["name", "type"]
        for f in br_table_fields:
            for k in required_keys:
                if k not in f.keys():
                    valid = False
                    raise KeyError(f"missing required key: {k}")
        valid_types = [
            "text",
            "long_text",
            "number",
            "date",
            "boolean",
            "link_row",
            "formula",
        ]
        for f in br_table_fields:
            if f["type"] not in valid_types:
                valid = False
                raise KeyError(f"invalid field type: {f['type']}")
            if f["type"] == "formula":
                if "formula" not in f.keys():
                    valid = False
                    raise KeyError("formula field missing 'formula' key")
                elif not isinstance(f["formula"], str):
                    valid = False
                    raise ValueError("formula field must be a string")
            if f["type"] == "link_row":
                if "link_row_table_id" not in f.keys():
                    valid = False
                    raise KeyError("link_row field missing 'link_row_table_id' key")
                elif not isinstance(f["link_row_table_id"], int):
                    valid = False
                    raise ValueError("link_row_table_id field must be a integer")
        return br_table_fields, valid

    def patch_row(self, table_id: str, row_id: str, payload: dict) -> dict:
        """sends a PATCH request for the given row

        Args:
            table_id (str): The ID of the table
            row_id (str): The ID of the row
            payload (dict): The patch-data, see https://api.baserow.io/api/redoc/#tag/Database-table-rows/operation/update_database_table_row

        Returns:
            dict: The JSON response of the updated row
        """  # noqa:

        url = f"{self.br_base_url}database/rows/table/{table_id}/{row_id}/?user_field_names=true"
        print(url)
        r = requests.patch(
            url,
            headers={
                "Authorization": f"Token {self.br_token}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        return r.json()

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
