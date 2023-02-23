import unittest
import os
import shutil
import glob
import json

from acdh_baserow_pyutils import BaseRowClient


TABLE_ID = "100948"
DATABASE_ID = "41426"

BASEROW_USER = os.environ.get("BASEROW_USER")
BASEROW_PW = os.environ.get("BASEROW_PW")
BASEROW_TOKEN = os.environ.get("BASEROW_TOKEN")
BR_CLIENT = BaseRowClient(BASEROW_USER, BASEROW_PW, BASEROW_TOKEN)


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
        self.assertEqual(len(tables), 3)

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
        files = BR_CLIENT.dump_tables_as_json(DATABASE_ID, folder_name=OUT_DIR, indent=4)
        for file in files:
            with open(file, 'r') as fp:
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
        with open("search.json", 'w') as f:
            json.dump(search_result, f)
        self.assertTrue(q in f"{search_result}")
        self.assertFalse(no_result in f"{search_result}")
        self.assertEqual(search_result["count"], 1)
