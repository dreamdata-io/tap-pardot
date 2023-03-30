import singer

from .streams import STREAM_OBJECTS
from .client import ENDPOINT_BASE, Client
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
    streams = [
        {
            "stream": "prospectAccountFields",
            "path": "prospectAccount/version/4/do/describe",
            "data_path": ["result", "field"],
        },
        {
            "stream": "customFields",
            "path": "customField/version/4/do/query",
            "data_path": ["result", "customField"],
        },
    ]
    for stream in streams:
        path = stream["path"]
        data = client._make_request(
            "GET",
            f"{ENDPOINT_BASE}{path}",
            params={"format": "json"},
        )
        records = get_data(data, stream["data_path"])
        singer.write_records(stream["stream"], records)


def get_data(data: Dict, path: List) -> Dict:
    if (not path) or (not data):
        return data
    for key in path:
        data = data.get(key, {})
        if not data:
            break
    return data
