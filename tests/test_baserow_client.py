import unittest
import os

from acdh_baserow_utils import BaseRowClient
from acdh_baserow_utils.utils import BaseRowUtilsError


TABLE_ID = "100948"
BR_TOKEN = os.environ.get('BASEROW_TOKEN')


class TestBaseRowClient(unittest.TestCase):
    """Tests for `acdh_baserow_utils` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_002_trailing_slash_check(self):

        br_base_url = 'whatever'
        br_client = BaseRowClient(
            br_base_url=br_base_url, br_token=br_base_url, br_table_id='1234'
        )
        self.assertEqual('/', br_client.br_base_url[-1])
        br_base_url = 'whatever/'
        br_client = BaseRowClient(
            br_base_url=br_base_url, br_token=br_base_url, br_table_id='1234'
        )
        self.assertEqual('/', br_client.br_base_url[-1])

    def test_003_check_table_id(self):

        br_base_url = 'whatever'
        br_client = BaseRowClient(
            br_base_url=br_base_url, br_token=br_base_url, br_table_id='1234'
        )
        self.assertTrue('1234' in br_client.br_rows_url)

    def test_003_check_missing_table_id(self):

        with self.assertRaises(BaseRowUtilsError):
            BaseRowClient()

    def test_004_iterate_rows(self):
        br_client = BaseRowClient(br_table_id=TABLE_ID)
        hansi = [x for x in br_client.yield_rows()]
        self.assertTrue('id' in hansi[0].keys())

    def test_999_missing_token(self):
        os.environ['BASEROW_TOKEN'] = 'NOT_SET'
        with self.assertRaises(BaseRowUtilsError):
            BaseRowClient(br_token=None)
