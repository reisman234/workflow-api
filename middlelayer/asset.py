
import os
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any

from middlelayer.models import ServiceDescription


class StaticAssetLoader():

    def __init__(self, **kwargs):
        kwargs.get("static_asset_directory")
        self.static_asset_directory = kwargs.get("static_asset_directory", "./assets")
        self.asset_info = list()
        self.assets_descriptions: Dict[str, ServiceDescription] = dict()
        self.__read_json_files()

    def __read_json_files(self):
        json_files = [file for file in os.listdir(self.static_asset_directory) if file.endswith('.json')]

        for file_name in json_files:
            file_path = os.path.join(self.static_asset_directory, file_name)
            (asset_id, _) = file_name.split(".")
            with open(file_path) as json_file:
                data = json.load(json_file)
                # Process the data from the JSON file

                self.asset_info.append({
                    "id": asset_id,
                    "start_date": datetime.now(),
                    "end_date:": datetime.now() + timedelta(days=7)})

                self.assets_descriptions[asset_id] = ServiceDescription(**data)

    def get_assets(self) -> Dict[str, Dict]:
        return self.asset_info

    def get_assets_description(self, asset_id):
        return self.assets_descriptions.get(asset_id)
