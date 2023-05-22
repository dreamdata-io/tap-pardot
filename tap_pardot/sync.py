import singer

from .streams import STREAM_OBJECTS
from .client import ENDPOINT_BASE, Client, parse_error
from typing import Dict, List

LOGGER = singer.get_logger()


def sync(client, config, state):
    for stream_id, stream_cls in STREAM_OBJECTS:
        stream_object = stream_cls(client, config, state)

        if stream_object is None:
            raise Exception("Attempted to sync unknown stream {}".format(stream_id))

        LOGGER.info("Syncing stream: " + stream_id)

        for rec in stream_object.sync():
            singer.write_record(stream_id, rec)


def sync_properties(client: Client):
    response = client._make_request(
        "GET",
        f"{ENDPOINT_BASE}prospectAccount/version/{client.api_version}/do/describe",
        params={"format": "json"},
    )
    _, code = parse_error(response)
    if code == 89:
        # You have requested version 4 of the API, but this account must use version 3
        client.api_version = 3
        response = client._make_request(
            "GET",
            f"{ENDPOINT_BASE}prospectAccount/version/{client.api_version}/do/describe",
            params={"format": "json"},
        )
    records = get_data(response.json(), ["result", "field"])
    singer.write_records("prospectAccountFields", records)


def get_data(data: Dict, path: List) -> Dict:
    if (not path) or (not data):
        return data
    for key in path:
        data = data.get(key, {})
        if not data:
            break
    return data
