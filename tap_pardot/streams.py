from datetime import datetime, timedelta
import inspect
import traceback
import singer
import sys

from tap_pardot.client import InvalidCredentials


LOGGER = singer.get_logger()


class Stream:
    stream_name = None
    data_key = None
    endpoint = None
    key_properties = ["id"]
    replication_keys = []
    replication_method = None
    is_dynamic = False

    client = None
    config = None
    state = None

    _last_bookmark_value = None

    def __init__(self, client, config, state, emit=True):
        self.client = client
        self.state = state
        self.config = config
        self.emit = emit

    def get_default_start(self):
        return self.config["start_date"]

    def get_params(self):
        return {}

    def get_bookmark(self):
        return (
            singer.bookmarks.get_bookmark(
                self.state, self.stream_name, self.replication_keys[0]
            )
            or self.get_default_start()
        )

    def update_bookmark(self, bookmark_value):
        singer.bookmarks.write_bookmark(
            self.state, self.stream_name, self.replication_keys[0], bookmark_value
        )
        if self.emit:
            singer.write_state(self.state)

    def pre_sync(self):
        """Function to run arbitrary code before a full sync starts."""

    def post_sync(self):
        """Function to run arbitrary code after a full sync completes."""
        singer.write_state(self.state)

    def get_records(self):
        data = self.client.get(self.endpoint, **self.get_params())

        if data.get("result") is None or data["result"].get("total_results") == 0:
            return []

        records = data["result"][self.data_key]
        if isinstance(records, dict):
            records = [records]

        for record in sorted(records, key=lambda x: x[self.replication_keys[0]]):
            yield self.flatten_value_records(record)

    def flatten_value_records(self, record):
        """In case when data comes as a dict with 'value' key only."""
        for key, value in record.items():
            if isinstance(value, dict) and "value" in value:
                record[key] = value["value"]
        return record

    def check_order(self, current_bookmark_value):
        if self._last_bookmark_value is None:
            self._last_bookmark_value = current_bookmark_value

        if current_bookmark_value < self._last_bookmark_value:
            raise Exception(
                "Detected out of order data. stream name: {} Current bookmark value {} is less than last bookmark value {}".format(
                    self.stream_name, current_bookmark_value, self._last_bookmark_value
                )
            )

        self._last_bookmark_value = current_bookmark_value

    def sync_page(self):
        for rec in self.get_records():
            current_bookmark_value = rec[self.replication_keys[0]]
            self.check_order(current_bookmark_value)
            self.update_bookmark(current_bookmark_value)
            yield rec

    def sync(self):
        self.pre_sync()

        try:
            records_synced = 0
            last_records_synced = -1
            while records_synced != last_records_synced:
                last_records_synced = records_synced
                for rec in self.sync_page():
                    records_synced += 1
                    yield rec
        except InvalidCredentials as e:
            LOGGER.error(
                "exception: %s \n traceback: %s",
                e,
                traceback.format_exc(),
            )
            sys.exit(5)
        except Exception as exc:
            LOGGER.error(
                "exception: %s \n traceback: %s",
                exc,
                traceback.format_exc(),
            )
            self.post_sync()
            sys.exit(1)

        self.post_sync()


class IdReplicationStream(Stream):
    """
    Streams where records are immutable and can only be sorted by id.

    Syncing mechanism:

    - use bookmark to keep track of the id
    - sync records since the last bookmarked id
    """

    replication_keys = ["id"]
    replication_method = "INCREMENTAL"

    def get_default_start(self):
        return 0

    def get_params(self):
        return {
            "id_greater_than": self.get_bookmark(),
            "sort_by": "id",
            "sort_order": "ascending",
        }


class CreatedAtReplicationStream(Stream):
    """
    Streams where records are immutable and can only be sorted by created_at.

    If no config is provided, it will try to fetch 10 years worth of data

    Syncing mechanism:

    - use bookmark to keep track of the created_at
    - sync records since the last bookmarked created_at
    """

    replication_keys = ["created_at"]
    replication_method = "INCREMENTAL"

    def get_default_start(self):
        return (datetime.now() - timedelta(days=10*365)).strftime('%Y-%m-%d %H:%M:%S')

    def get_params(self):
        return {
            "created_after": self.get_bookmark(),
            "sort_by": "created_at",
            "sort_order": "ascending",
        }


class UpdatedAtReplicationStream(Stream):
    """
    Streams where records are mutable, can be sorted by updated_at, and return
    updated_at.

    Syncing mechanism:

    - use bookmark to keep track of last updated_at
    - sync records since the last bookmarked updated_at
    """

    replication_keys = ["updated_at"]
    replication_method = "INCREMENTAL"

    def get_params(self):
        return {
            "updated_after": self.get_bookmark(),
            "sort_by": "updated_at",
            "sort_order": "ascending",
        }


class ComplexBookmarkStream(Stream):
    """Streams that need to keep track of more than 1 bookmark."""

    def get_default_start(self, key):
        defaults = {
            "updated_at": self.config["start_date"],
            "last_updated": self.config["start_date"],
            "id": 0,
            "offset": 0,
        }
        return defaults.get(key)

    def clear_bookmark(self, bookmark_key):
        singer.bookmarks.clear_bookmark(self.state, self.stream_name, bookmark_key)
        if self.emit:
            singer.write_state(self.state)

    def get_bookmark(self, bookmark_key):
        return singer.bookmarks.get_bookmark(
            self.state, self.stream_name, bookmark_key
        ) or self.get_default_start(bookmark_key)

    def update_bookmark(self, bookmark_key, bookmark_value):
        singer.bookmarks.write_bookmark(
            self.state, self.stream_name, bookmark_key, bookmark_value
        )
        if self.emit:
            singer.write_state(self.state)

    def sync_page(self):
        raise NotImplementedError("ComplexBookmarkStreams need a custom sync method.")


class NoUpdatedAtSortingStream(ComplexBookmarkStream):
    """
    Streams that can't sort by updated_at but have an updated_at field returned.

    Syncing mechanism:

    - get last updated_at bookmark
    - start full sync by id, starting at 0 and using id bookmark for paging
    - only emit records that have been updated since last sync
    - while iterating thorugh records, keep track of the max updated_at
    - when sync is finished, update the updated_at bookmark with max_updated_at
    """

    replication_keys = ["id", "updated_at"]
    replication_method = "INCREMENTAL"

    max_updated_at = None
    last_updated_at = None

    def __init__(self, *args, **kwargs):
        super(NoUpdatedAtSortingStream, self).__init__(*args, **kwargs)
        self.last_updated_at = self.get_bookmark("updated_at")
        self.max_updated_at = self.last_updated_at

    def post_sync(self):
        self.clear_bookmark("id")
        self.update_bookmark("updated_at", self.max_updated_at)
        super(NoUpdatedAtSortingStream, self).post_sync()

    def get_params(self):
        return {
            "created_after": self.config["start_date"],
            "id_greater_than": self.get_bookmark("id"),
            "sort_by": "id",
            "sort_order": "ascending",
        }

    def sync_page(self):
        for rec in self.get_records():
            current_id = rec["id"]
            if self.last_updated_at and rec["updated_at"] <= self.last_updated_at:
                continue

            self.check_order(current_id)
            self.max_updated_at = max(self.max_updated_at, rec["updated_at"])
            self.update_bookmark("id", current_id)
            yield rec


class UpdatedAtSortByIdReplicationStream(ComplexBookmarkStream):
    """
    Streams that don't return an updated_at field but can be queried using
    updated_after.

    Syncing mechanism:

    - when a full sync starts, store current time in sync_start_time bookmark
    - if that bookmark exists, then we haven't finished a full sync and it'll
      pick up from where it left off.
    - use a last_updated bookmark to sync items updated_after last sync
    - start each full sync with id = 0 and sync all newly updated records paging
      by an id bookmark
    - when ful sync finishes, delete teh sync_start_time and id bookmarks and update
      last_updated bookmark to the sync_start_time
    """

    replication_keys = ["id"]
    replication_method = "INCREMENTAL"

    start_time = None

    def pre_sync(self):
        self.start_time = self.get_bookmark("sync_start_time")

        if self.start_time is None:
            self.start_time = singer.utils.strftime(singer.utils.now())
            self.update_bookmark("sync_start_time", self.start_time)
        super(UpdatedAtSortByIdReplicationStream, self).pre_sync()

    def post_sync(self):
        self.clear_bookmark("sync_start_time")
        self.clear_bookmark("id")
        self.update_bookmark("last_updated", self.start_time)
        super(UpdatedAtSortByIdReplicationStream, self).post_sync()

    def get_params(self):
        return {
            "id_greater_than": self.get_bookmark("id"),
            "updated_after": self.get_bookmark("last_updated"),
            "sort_by": "id",
            "sort_order": "ascending",
        }

    def sync_page(self):
        for rec in self.get_records():
            current_id = rec["id"]
            self.check_order(current_id)
            self.update_bookmark("id", current_id)
            yield rec


class ChildStream(ComplexBookmarkStream):
    parent_class = None
    parent_id_param = None

    def pre_sync(self):
        self.parent_bookmark = self.get_bookmark("parent_bookmark")

        if self.parent_bookmark is None:
            self.parent_bookmark = {}
            self.update_bookmark("parent_bookmark", self.parent_bookmark)
        super(ChildStream, self).pre_sync()

    def post_sync(self):
        self.clear_bookmark("parent_bookmark")
        super(ChildStream, self).post_sync()

    def get_params(self):
        return {"offset": self.get_bookmark("offset")}

    def get_records(self, *parent_ids):
        params = {
            self.parent_id_param: ",".join([str(x) for x in parent_ids]),
            **self.get_params(),
        }

        data = self.client.post(self.endpoint, **params)
        self.update_bookmark("offset", params.get("offset", 0) + 200)

        if (
            data["result"] is None
            or data["result"].get("total_results") == 0
            or data["result"].get(self.data_key) == None
        ):
            return []

        records = data["result"][self.data_key]
        if isinstance(records, dict):
            records = [records]

        return records

    def sync_page(self, parent_ids):
        for rec in self.get_records(*parent_ids):
            yield rec

    def get_parent_ids(self, parent):
        while True:
            parent_ids = [rec["id"] for rec in parent.sync_page()]
            if len(parent_ids):
                yield parent_ids
                self.update_bookmark("parent_bookmark", self.parent_bookmark)
            else:
                break

    def sync(self):
        self.pre_sync()

        parent = self.parent_class(
            self.client, self.config, self.parent_bookmark, emit=False
        )

        for parent_ids in self.get_parent_ids(parent):
            records_synced = 0
            last_records_synced = -1

            while records_synced != last_records_synced:
                last_records_synced = records_synced
                for rec in self.sync_page(parent_ids):
                    records_synced += 1
                    yield rec
            self.clear_bookmark("offset")

        self.post_sync()


class EmailClicks(IdReplicationStream):
    stream_name = "email_clicks"
    data_key = "emailClick"
    endpoint = "emailClick"

    is_dynamic = False


class VisitorActivities(CreatedAtReplicationStream):
    stream_name = "visitor_activities"
    data_key = "visitor_activity"
    endpoint = "visitorActivity"
    # We've encountered a situation where we have been unable to finish the sync due
    # to fetching too much data. Data Sciense is only using some types of visitor 
    # activities. Hence, we can filter out the used ones only.
    filter_types = "1,2,4,6,17,21,24,25,26,27,28,29,34"
    datetime_format = "%Y-%m-%d %H:%M:%S"

    def get_params(self):
        p = CreatedAtReplicationStream.get_params(self)

        # In order to avoid timeouts, we need to drasticaly limit the amount of activities that we
        # ask Pardot to process per request.
        cb = datetime.strptime(p["created_after"], self.datetime_format) + timedelta(days=7)

        p.update(
            type=self.filter_types,
            created_before=cb.strftime(self.datetime_format)
        )

        return p 

    def sync(self):
        self.pre_sync()

        try:
            now = datetime.now()

            # Since we're now synchronizing visitor activities in timed windows, we need to account
            # for the case where a given window has no data.
            while True:
                # Since the loop in practice relies on the created_at found in the bookmarks, we
                # need to actually short circuit break when there are no more records.
                n = 0

                for rec in self.sync_page():
                    n += 1
                    yield rec

                if n == 0 and now < datetime.strptime(self.get_params()["created_before"], self.datetime_format):
                    break
        except InvalidCredentials as e:
            LOGGER.error(
                "exception: %s \n traceback: %s",
                e,
                traceback.format_exc(),
            )
            sys.exit(5)
        except Exception as exc:
            LOGGER.error(
                "exception: %s \n traceback: %s",
                exc,
                traceback.format_exc(),
            )
            self.post_sync()
            sys.exit(1)

        self.post_sync()

    is_dynamic = False


class ProspectAccounts(UpdatedAtReplicationStream):
    stream_name = "prospect_accounts"
    data_key = "prospectAccount"
    endpoint = "prospectAccount"

    is_dynamic = True


class Prospects(UpdatedAtReplicationStream):
    stream_name = "prospects"
    data_key = "prospect"
    endpoint = "prospect"

    is_dynamic = False

    def sync_page(self):
        bookmark = self.get_bookmark()
        params = {
            **self.get_params(),
            "offset": 0,
        }

        while True:
            data = self.client.get(self.endpoint, **params)
            records = data["result"].get(self.data_key)

            if not records:
                break
            if isinstance(records, dict):
                records = [records]

            for record in sorted(records, key=lambda x: x[self.replication_keys[0]]):
                bookmark = record[self.replication_keys[0]]

                yield self.flatten_value_records(record)

            params["offset"] += 200

            self.update_bookmark(bookmark)


class Opportunities(NoUpdatedAtSortingStream):
    stream_name = "opportunities"
    data_key = "opportunity"
    endpoint = "opportunity"

    is_dynamic = False


class Users(NoUpdatedAtSortingStream):
    stream_name = "users"
    data_key = "user"
    endpoint = "user"

    is_dynamic = False


class Visitors(UpdatedAtReplicationStream):
    stream_name = "visitors"
    data_key = "visitor"
    endpoint = "visitor"

    is_dynamic = False

    def get_params(self):
        return {
            "updated_after": self.get_bookmark(),
            "sort_by": "updated_at",
            "sort_order": "ascending",
            "only_identified": "false",
        }

    def sync_page(self):
        for rec in self.get_records():
            current_bookmark_value = rec[self.replication_keys[0]]

            if self._last_bookmark_value is None:
                self._last_bookmark_value = current_bookmark_value

            # This is possibly the most asinine edge-case/bug I've ever run into.
            # In Pardot, for the visitors stream, there seems to _sometimes_ exist certain timespans for which the API
            # returns data _an hour before_ the specified time.
            #
            # To put it more literally, given the following cURL:
            #
            # curl -X GET -G 'https://pi.pardot.com/api/visitor/version/4/do/query' \
            #   --data-urlencode 'updated_after=2020-11-01 01:00:00' \
            #   --data 'sort_by=updated_at' \
            #   --data 'sort_order=ascending' \
            #   --data 'only_identified=false' \
            #   --data 'limit=1' \
            #   --header "Authorization: Bearer $TOKEN" \
            #   --header "Pardot-Business-Unit-Id: $PBUID"
            #
            # The returned <visitor> will contain a <updated_at> sub-element, which will contain a datetime _before_
            # the specified `updated_at` query parameter - in the case of 2020-11-01 01:00:00 for siteimprove_com, this
            # was 2020-11-01 00:00:01.
            #
            # In effect, anytime the tap crosses into the 01:00:00-02:00:00 timerange, the API returns data in the
            # 00:00:00-01:00:00 timerange.
            # As a consequence of this, we're completely unable to get this data.
            #
            # This "quirk" is confirmed for the following timeranges for siteimprove_com:
            #
            #   2020-11-01 01:00:00 - 2020-11-01 02:00:00
            #   2018-11-04 01:00:00 - 2018-11-04 02:00:00
            #
            # There are likely to be more timeranges for siteimprove_com.
            # It's uncertain whether this effects any other customers.
            #
            # This is insane, but what more can you expect from a billion dollar company, am I right?
            if current_bookmark_value < self._last_bookmark_value:
                singer.log_warning(
                    "Detected out of order visitor, this is likely related to a bug in Pardot - see source code for more information."
                )
                continue

            self._last_bookmark_value = current_bookmark_value

            self.update_bookmark(current_bookmark_value)
            yield rec


class Visits(ChildStream, NoUpdatedAtSortingStream):
    stream_name = "visits"
    data_key = "visit"
    endpoint = "visit"

    is_dynamic = False

    parent_class = Visitors
    parent_id_param = "visitor_ids"

    def pre_sync(self):
        self.parent_bookmark = self.get_bookmark("parent_bookmark")

        if self.parent_bookmark is None:
            self.parent_bookmark = {
                "bookmarks": {
                    self.parent_class.stream_name: {
                        "updated_at": self.get_bookmark("updated_at")
                    }
                }
            }

            self.update_bookmark("parent_bookmark", self.parent_bookmark)
        super(ChildStream, self).pre_sync()

    def fix_page_views(self, record):
        page_views = (record.get("visitor_page_views") or {}).get("visitor_page_view")
        if isinstance(page_views, dict):
            record["visitor_page_views"]["visitor_page_view"] = [page_views]

    def sync_page(self, parent_ids):
        """
        Visits uses offset to paginate through.

        This is handled in ChildStream base class.
        """
        for rec in self.get_records(*parent_ids):
            if rec["updated_at"] <= self.last_updated_at:
                continue
            self.fix_page_views(rec)
            self.max_updated_at = max(self.max_updated_at, rec["updated_at"])
            yield rec


class Lists(UpdatedAtReplicationStream):
    stream_name = "lists"
    data_key = "list"
    endpoint = "list"

    is_dynamic = False


class ListMemberships(ChildStream, NoUpdatedAtSortingStream):
    stream_name = "list_memberships"
    data_key = "list_membership"
    endpoint = "listMembership"

    is_dynamic = False

    parent_class = Lists
    parent_id_param = "list_id"

    replication_keys = ["id", "updated_at", "list_id"]
    replication_method = "INCREMENTAL"

    def get_params(self):
        """ListMemberships use id to paginate through, so we override ChildStream
        behavior."""
        return {
            # Even though we can't sort by updated_at, we can
            # filter by updated_after
            "updated_after": self.get_bookmark("updated_at")
            or self.config["start_date"],
            "id_greater_than": self.get_bookmark("id") or 0,
            "sort_by": "id",
            "sort_order": "ascending",
        }

    def get_parent_ids(self, parent):
        """ListMemberships take only 1 parent id at a time."""
        records_synced = 0
        last_records_synced = -1

        while records_synced != last_records_synced:
            last_records_synced = records_synced
            for rec in parent.sync_page():
                records_synced += 1
                yield rec["id"]
                self.update_bookmark("parent_bookmark", self.parent_bookmark)

    def sync_page(self, parent_id):
        """ListMemberships use id to paginate through, so we override ChildStream
        behavior."""
        for rec in self.get_records(parent_id):
            if rec["updated_at"] <= self.last_updated_at:
                continue
            self.max_updated_at = max(self.max_updated_at, rec["updated_at"])
            self.update_bookmark("id", rec["id"])
            yield rec


class Campaigns(UpdatedAtSortByIdReplicationStream):
    stream_name = "campaigns"
    data_key = "campaign"
    endpoint = "campaign"

    is_dynamic = False


STREAM_OBJECT_MAP = {
    cls.stream_name: cls
    for cls in globals().values()
    if inspect.isclass(cls) and issubclass(cls, Stream) and cls.stream_name
}
STREAM_OBJECTS = [
    (stream_name, STREAM_OBJECT_MAP.pop(stream_name))
    for stream_name in [
        "prospect_accounts",
        "prospects",
        "campaigns",
        "visitor_activities",
        "visits",
        "email_clicks",
        "opportunities",
        "users",
        "visitors",
        "lists",
        "list_memberships",
    ]
]
if STREAM_OBJECT_MAP:
    raise RuntimeError(
        f"streams found that is not part of the ordered STREAM_OBJECTS: {list(STREAM_OBJECT_MAP.keys())}"
    )
