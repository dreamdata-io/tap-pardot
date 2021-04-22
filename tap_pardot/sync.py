import singer

from .streams import STREAM_OBJECTS

LOGGER = singer.get_logger()


def sync(client, config, state):
    for stream_id, stream_cls in STREAM_OBJECTS:
        stream_object = stream_cls(client, config, state)

        if stream_object is None:
            raise Exception("Attempted to sync unknown stream {}".format(stream_id))

        LOGGER.info("Syncing stream: " + stream_id)

        for rec in stream_object.sync():
            singer.write_record(stream_id, rec)
