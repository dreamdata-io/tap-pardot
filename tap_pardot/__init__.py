#!/usr/bin/env python3
import sys

import singer
from singer import utils

from tap_pardot.client import Client, InvalidCredentials
from tap_pardot.sync import sync, sync_properties

LOGGER = singer.get_logger()


REQUIRED_CONFIG_KEYS = [
    "start_date",
    "refresh_token",
    "client_id",
    "client_secret",
    "business_unit_id",
]


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    client = Client(**args.config)

    LOGGER.info("Starting sync mode")
    try:
        sync_properties(client)
    except InvalidCredentials as e:
        LOGGER.exception(e)
        sys.exit(5)
    sync(client, args.config, args.state)


if __name__ == "__main__":
    main()
