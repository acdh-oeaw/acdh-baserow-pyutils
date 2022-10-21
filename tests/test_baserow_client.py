import unittest
import os

from acdh_baserow_utils import BaseRowClient


TABLE_ID = "100948"
DATABASE_ID = "41426"

BASEROW_USER = os.environ.get('BASEROW_USER')
BASEROW_PW = os.environ.get('BASEROW_PW')
BASEROW_TOKEN = os.environ.get('BASEROW_TOKEN')
BR_CLIENT = BaseRowClient(BASEROW_USER, BASEROW_PW, BASEROW_TOKEN)


class TestBaseRowClient(unittest.TestCase):
    """Tests for `acdh_baserow_utils` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_001_iterate_rows(self):
        hansi = [x for x in BR_CLIENT.yield_rows(TABLE_ID)]
        self.assertTrue('id' in hansi[0].keys())

    def test_002_list_tables(self):
        tables = BR_CLIENT.list_tables(DATABASE_ID)
        self.assertEqual(len(tables), 3)

    def test_003_fix_url(self):
        url_fixer = BR_CLIENT.url_fixer('hansi')
        self.assertEqual(url_fixer[-1], '/')
