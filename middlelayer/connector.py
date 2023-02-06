import requests

import time
from functools import wraps


def retry_on_failure(max_retries: int, delay: int):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(max_retries):
                result = func(*args, **kwargs)
                if result:
                    return result
                time.sleep(delay)
            return None
        return wrapper
    return decorator


class EDCConnector:
    def __init__(self, base_url: str, provider_ids: str, headers: list):
        self.base_url = base_url
        self.provider_ids = provider_ids
        self.headers = headers

    def __make_request(self, endpoint: str, method: str, data: dict = None):
        url = self.base_url + endpoint
        try:
            if method == "GET":
                response = requests.get(url)
            elif method == "POST":
                headers = {'Content-type': 'application/json'}
                response = requests.post(url, json=data, headers=headers)
            elif method == "PUT":
                response = requests.put(url, json=data)
            elif method == "DELETE":
                response = requests.delete(url)
            else:
                raise ValueError(f"Invalid method: {method}")
            response.raise_for_status()
            if response.status_code == 404:
                return None

            return response.json()
        except requests.exceptions.HTTPError as http_err:
            if response.status_code >= 400 and response.status_code < 500:
                print(f'4xx Error: {http_err}')
                return None
            elif response.status_code >= 500:
                print(f'5xx Error: {http_err}')
                return None

    def get_catalog(self):
        endpoint = "catalog/?providerUrl={provider_ids}/data"
        return self.__make_request(endpoint, "GET")

    def negotiate_offer(self, offer_data: dict):
        endpoint = "contractnegotiations"
        negotiate_response = self.__make_request(endpoint, "POST", offer_data)

        negotiation_id = negotiate_response["id"]
        self.__check_negotiation_state(negotiation_id)
        return

    def __check_negotiation_state(self, negotiation_id):
        endpoint = f"contractnegotiations/{negotiation_id}"
        status_response = self.__make_request(endpoint, "GET")
        status_response["TODO"]

    def trigger_transfer(self, transfer_data: dict):
        # TODO handle wrong transfer state of edc
        endpoint = "transferprocess"
        transfer_response = self.__make_request(endpoint, "POST", transfer_data)



    def __get_requst_offer(self, provider_ids, offer_id, policy_id, asset_id):
        negotiation_offer = {
            "connectorId": "urn:connector:edc",
            "connectorAddress": f"{provider_ids}/data",
            "protocol": "ids-multipart",
            "offer": {
                "offerId": offer_id,
                "assetId": asset_id,
                "policy": {
                        "uid": policy_id,
                        "permissions": [
                            {
                                "edctype": "dataspaceconnector:permission",
                                "uid": None,
                                "target": asset_id,
                                "action": {
                                    "type": "USE",
                                    "includedIn": None,
                                    "constraint": None
                                },
                                "assignee": None,
                                "assigner": None,
                                "constraints": [],
                                "duties": []
                            }
                        ],
                    "prohibitions": [],
                    "obligations": [],
                    "extensibleProperties": {},
                    "inheritsFrom": None,
                    "assigner": None,
                    "assignee": None,
                    "target": None,
                    "@type": {
                            "@policytype": "set"
                    }
                },
                "asset": {
                    "properties": {
                        "ids:byteSize": None,
                        "asset:prop:id": asset_id,
                        "ids:fileName": None
                    }
                }
            }
        }

    def __get_http_data_destination():

        HTTP_DATA_BASE_URL = "http://192.52.44.77:18001/rosbag/"

        data_destination = {
            "properties": {
                "baseUrl": HTTP_DATA_BASE_URL,
                "type": "HttpData"
            }
        }
        return data_destination

    def __get_s3_data_destination():

        AWS_TARGET_REGION = "local"
        AWS_TARGET_BUCKET_NAME = "imlabucket"
        AWS_TARGET_PATH = "output"
        AWS_TARGET_ACCESS_KEY = "root"
        AWS_TARGET_SECRET_KEY = "changeme123"
        AWS_TARGET_ENDPOINT_OVERRIDE = "http://proto-minio:9000"

        data_destination = {
            "properties": {
                "type": "AmazonS3",
                "region": AWS_TARGET_REGION,
                "keyName": AWS_TARGET_PATH,
                "bucketName": AWS_TARGET_BUCKET_NAME,
                "accessKeyId": AWS_TARGET_ACCESS_KEY,
                "secretAccessKey": AWS_TARGET_SECRET_KEY
            },
            "type": "AmazonS3"
        }
        if AWS_TARGET_ENDPOINT_OVERRIDE:
            data_destination["properties"]["endpointOverride"] = AWS_TARGET_ENDPOINT_OVERRIDE

        return data_destination

    def __get_transfer_details(asset_id, agreement_id, provider_ids, data_destination):
        transfer_data = {
            "protocol": "ids-multipart",
            "assetId": asset_id,
            "contractId": agreement_id,
            "dataDestination": data_destination,
            "transferType": {
                "contentType": "application/octet-stream",
                "isFinite": True
            },
            "managedResources": False,
            "connectorAddress": f"{provider_ids}/data",
            "connectorId": "urn:connector:edc"
        }
