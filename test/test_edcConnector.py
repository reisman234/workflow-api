import unittest
from unittest.mock import patch

from middlelayer.connector import EDCConnector


class TestEDCConnector(unittest.TestCase):
    @patch('middlelayer.connector.requests')
    def test_get_catalog(self, mock_requests):
        # setup
        mock_requests.get.return_value.status_code = 200
        mock_requests.get.return_value.json.return_value = {
            'items': ['item1', 'item2']}
        provider_ids = "http://edc-provider.com/ids/"
        edc = EDCConnector("https://edc.com/api/", provider_ids, headers=[])

        # exercise
        result = edc.get_catalog()

        # verify
        mock_requests.get.assert_called_once_with(
            "https://edc.com/api/catalog/?providerUrl={provider_ids}/data")
        self.assertEqual(result, {'items': ['item1', 'item2']})

    @patch('middlelayer.connector.requests')
    def test_get_catalog_error(self, mock_requests):
        # setup
        mock_requests.get.return_value.status_code = 404
        provider_ids = "http://edc-provider.com/ids/"
        edc = EDCConnector("https://edc.com/api/", provider_ids, headers=[])

        # exercise
        result = edc.get_catalog()

        # verify
        mock_requests.get.assert_called_once_with(
            "https://edc.com/api/catalog/?providerUrl={provider_ids}/data")
        self.assertEqual(result, None)


if __name__ == '__main__':
    unittest.main()
