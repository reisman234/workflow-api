
import unittest

from middlelayer.asset import StaticAssetLoader


class TestStaticAssetLoader(unittest.TestCase):

    def setUp(self) -> None:
        self.testee = StaticAssetLoader(static_asset_directory="./config/assets")

    def test_get_static_assets(self):
        compute_assets = self.testee.get_assets()

        self.assertIsInstance(compute_assets, list)
        self.assertTrue(len(compute_assets) >= 1, "compute_assets was empty")

        print(compute_assets)
        service_ids = []
        for x in compute_assets:
            service_ids.append(x.get("id"))
        print(service_ids)

        self.assertIn("dummy", service_ids)

    def test_get_dummy_asset_description(self):
        description = self.testee.get_assets_description("dummy")

        self.assertIsNotNone(description)
        self.assertEqual("dummy", description.service_id, "asset description id not equal")
