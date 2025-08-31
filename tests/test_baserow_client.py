import csv
import glob
import json
import os
import shutil
import unittest
from datetime import datetime

from acdh_baserow_pyutils import BaseRowClient, get_related_table_info

from .config import BASEROW_TABLE_MAPPING

TABLE_ID = "100948"
DATABASE_ID = "41426"

BASEROW_USER = os.environ.get("BASEROW_USER")
BASEROW_PW = os.environ.get("BASEROW_PW")
BASEROW_TOKEN = os.environ.get("BASEROW_TOKEN")
BR_CLIENT = BaseRowClient(BASEROW_USER, BASEROW_PW, BASEROW_TOKEN)
BR_CLIENT_WITH_DB_ID = BaseRowClient(
    BASEROW_USER, BASEROW_PW, BASEROW_TOKEN, br_db_id=DATABASE_ID
)

table_name = "person"
field_name = "Name"
q = "Hansi4ever"


class TestBaseRowClient(unittest.TestCase):
    """Tests for `acdh_baserow_utils` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_001_iterate_rows(self):
        hansi = [x for x in BR_CLIENT.yield_rows(TABLE_ID)]
        self.assertTrue("id" in hansi[0].keys())

    def test_002_list_tables(self):
        tables = BR_CLIENT.list_tables(DATABASE_ID)
        self.assertEqual(len(tables), 4)

    def test_003_fix_url(self):
        url_fixer = BR_CLIENT.url_fixer("hansi")
        self.assertEqual(url_fixer[-1], "/")

    def test_004_dump_data(self):
        OUT_DIR = "out"
        os.makedirs(OUT_DIR, exist_ok=True)
        files = BR_CLIENT.dump_tables_as_json(DATABASE_ID, folder_name=OUT_DIR)
        file_list = glob.glob(f"{OUT_DIR}/*.json")
        self.assertEqual(len(files), len(file_list))
        shutil.rmtree(OUT_DIR)

    def test_005_dump_data_with_indent(self):
        OUT_DIR = "out"
        os.makedirs(OUT_DIR, exist_ok=True)
        files = BR_CLIENT.dump_tables_as_json(
            DATABASE_ID, folder_name=OUT_DIR, indent=4
        )
        for file in files:
            with open(file, "r") as fp:
                line_count = len(fp.readlines())
                self.assertTrue(line_count > 1)
        shutil.rmtree(OUT_DIR)

    def test_006_list_fields(self):
        fields = BR_CLIENT.list_fields(TABLE_ID)
        field = fields[0]
        self.assertEqual(field["primary"], True)

    def test_007_search(self):
        q = "Susi"
        no_result = "Schnitzler"
        search_result = BR_CLIENT.search_rows(TABLE_ID, q, "631801")
        with open("search.json", "w") as f:
            json.dump(search_result, f)
        self.assertTrue(q in f"{search_result}")
        self.assertFalse(no_result in f"{search_result}")
        self.assertEqual(search_result["count"], 1)

    def test_008_get_table_by_name(self):
        place_table = BR_CLIENT.get_table_by_name(DATABASE_ID, "place")
        self.assertEqual(place_table, BASEROW_TABLE_MAPPING["place"])
        place_table = BR_CLIENT.get_table_by_name(DATABASE_ID, "asdf")
        self.assertFalse(place_table)

    def test_009_table_dict(self):
        field_name = "Name"
        br_client = BR_CLIENT_WITH_DB_ID
        field_name_value = br_client.br_table_dict["person"]["fields"][field_name]
        self.assertEqual(field_name, field_name_value["name"])

    def test_010_get_or_create(self):
        br_client = BR_CLIENT_WITH_DB_ID
        object, _ = br_client.get_or_create(
            table_name, field_name, br_client.br_table_dict, q
        )
        self.assertTrue(object[field_name], q)

    def test_011_create_delete_tables_and_fields(self):
        br_client = BR_CLIENT_WITH_DB_ID
        table_name = "test_table"
        table, created = br_client.create_table(table_name)
        self.assertEqual(created, True)
        self.assertTrue("id" in table.keys())
        table_name2 = "test_table2"
        table_fields = [
            ["test_field1", "test_field2", "test_field3"],
            ["value1", "value2", "value3"],
        ]
        table2, created2 = br_client.create_table(table_name2, fields=table_fields)
        self.assertEqual(created2, True)
        self.assertTrue("id" in table2.keys())
        field_names = [
            {"name": "test_field", "type": "text"},
            {"name": "test_field2", "type": "long_text"},
            {
                "name": "test_field3",
                "type": "formula",
                "formula": "concat(field('test_field', field('test_field2'))",
            },
            {
                "name": "test_field4",
                "type": "link_row",
                "link_row_table_id": table["id"],
                "has_related_field": False,
            },
            {
                "name": "test_field5",
                "type": "link_row",
                "link_row_table_id": table2["id"],
                "has_related_field": True,
            },
            {"name": "test_field6", "type": "boolean"},
            {"name": "test_field7", "type": "number"},
            {"name": "test_field8", "type": "date"},
        ]
        try:
            field, created = br_client.create_table_fields(table["id"], field_names)
            self.assertEqual(created, True)
            self.assertTrue("id" in field.keys())
        except (ValueError, KeyError):
            self.assertFalse(False)
        field, deleted = br_client.delete_table_fields(
            table["id"], ["test_field", "test_field2"]
        )
        self.assertEqual(deleted, True)
        self.assertTrue("related_fields" in field.keys())
        table, deleted = br_client.delete_table(table["id"])
        self.assertEqual(deleted, True)
        self.assertTrue("status" in table.keys())
        table, deleted = br_client.delete_table(table2["id"])
        self.assertEqual(deleted, True)
        self.assertTrue("status" in table.keys())

    def test_012_validate_table_fields_type(self):
        br_table_fields = [{"name": "test_field"}]
        with self.assertRaises(KeyError):
            br_table_fields, valid = BR_CLIENT.validate_table_fields_type(
                br_table_fields
            )
        br_table_fields = [{"type": "long_text"}]
        with self.assertRaises(KeyError):
            br_table_fields, valid = BR_CLIENT.validate_table_fields_type(
                br_table_fields
            )
        br_table_fields = [{"name": "test_field3", "type": "formula"}]
        with self.assertRaises(KeyError):
            br_table_fields, valid = BR_CLIENT.validate_table_fields_type(
                br_table_fields
            )
        br_table_fields = [
            {
                "name": "test_field3",
                "type": "formula",
                "formula": ["concat(field('test_field', field('test_field2'))"],
            }
        ]
        with self.assertRaises(ValueError):
            br_table_fields, valid = BR_CLIENT.validate_table_fields_type(
                br_table_fields
            )
        br_table_fields = [
            {
                "name": "test_field4",
                "type": "link_row",
                "link_row_table_id": "1",
                "has_related_field": False,
            }
        ]
        with self.assertRaises(ValueError):
            br_table_fields, valid = BR_CLIENT.validate_table_fields_type(
                br_table_fields
            )
        br_table_fields = [
            {
                "name": "test_field5",
                "type": "link_row",
            }
        ]
        with self.assertRaises(KeyError):
            br_table_fields, valid = BR_CLIENT.validate_table_fields_type(
                br_table_fields
            )

    def test_013_get_related_table_info(self):
        br_client = BR_CLIENT_WITH_DB_ID
        table_field_dict = br_client.br_table_dict
        related_table_name = get_related_table_info(
            "person", "born_in", table_field_dict
        )
        self.assertEqual(related_table_name[1], "place")

    def test_014_path_row(self):
        table_id = "100948"
        row_id = "1"
        patched_value = 1
        payload = {"died_in": [patched_value]}
        patched = BR_CLIENT.patch_row(table_id, row_id, payload=payload)
        death_place = patched["died_in"][0]["id"]
        self.assertEqual(death_place, patched_value)
        patched_value = 2
        payload = {"died_in": [patched_value]}
        patched = BR_CLIENT.patch_row(table_id, row_id, payload=payload)
        death_place = patched["died_in"][0]["id"]
        self.assertEqual(death_place, patched_value)

    def test_015_batch_update(self):
        table_id = "100948"
        current_time = datetime.now()
        data = os.path.join("tests", "batch_update.csv")
        items = []
        with open(data, encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                try:
                    item = {"id": int(row[0]), "Projekt": f"foo {current_time}"}
                except ValueError:
                    continue
                items.append(item)
        items = items + [{"id": 99999, "Projekt": "lakdsfj"}]
        BR_CLIENT.batch_update_rows(table_id, items)
