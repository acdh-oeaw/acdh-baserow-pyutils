import unittest

from acdh_baserow_utils import BaseRowClient
from acdh_baserow_utils.utils import NoTokenFound


class TestBaseRowClient(unittest.TestCase):
    """Tests for `acdh_baserow_utils` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_001_missing_token(self):
        with self.assertRaises(NoTokenFound):
            BaseRowClient()

    def test_002_trailing_slash_check(self):
        br_url = 'whatever'
        br_client = BaseRowClient(baserow_url=br_url, baserow_token=br_url)
        self.assertEqual('/', br_client.baserow_url[-1])
        br_url = 'whatever/'
        br_client = BaseRowClient(baserow_url=br_url, baserow_token=br_url)
        self.assertEqual('/', br_client.baserow_url[-1])
