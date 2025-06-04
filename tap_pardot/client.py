import backoff
import requests
import singer
from typing import Dict, Tuple, cast

LOGGER = singer.get_logger()

AUTH_URL = "https://pi.pardot.com/api/login/version/3"
ENDPOINT_BASE = "https://pi.pardot.com/api/"

# Smallest Pardot package has a 25k request limit, so we leave some room
# for other integrations to function.
REQUEST_LIMIT = 20000
PAGE_SIZE = 200


def parse_error(response: requests.Response) -> Tuple[str, int]:
    error: str
    code: int
    if response.headers.get("content-type") != "application/json":
        code = response.status_code
        error = "PardotAPIError: " + response.text
    else:
        data: Dict = response.json()
        code = cast(int, data.get("@attributes", {}).get("err_code"))
        error = cast(str, data.get("err"))

    return error, code


class PardotException(Exception):
    def __init__(self, response: requests.Response):
        message, self.code = parse_error(response)

        self.url = response.request.url
        self.method = response.request.method
        self.raw = response.text

        super().__init__(message)


class InvalidCredentials(Exception):
    pass


class RateLimitException(Exception):
    pass


class Client:
    access_token = None
    refresh_token = None
    client_id = None
    client_secret = None
    business_unit_id = None

    get_url = "{}/version/{}/do/query"

    num_requests = 0
    request_limit = REQUEST_LIMIT

    def __init__(
        self,
        business_unit_id,
        client_id,
        client_secret,
        refresh_token,
        access_token=None,
        **kwargs,
    ):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.business_unit_id = business_unit_id
        self.requests_session = requests.Session()
        self.api_version = 4
        self._set_limit()

    def _get_auth_header(self):
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Pardot-Business-Unit-Id": self.business_unit_id,
        }

    @backoff.on_exception(
        backoff.expo,
        (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            PardotException,
        ),
        jitter=None,
        max_tries=10,
    )
    def _make_request(
        self, method, url, params=None, data=None, activity=None
    ) -> requests.Response:
        LOGGER.info(
            "%s - Making request to %s endpoint %s, with params %s",
            url,
            method.upper(),
            url,
            params,
        )

        if self.num_requests >= self.request_limit:
            raise RateLimitException("Reach daily quota usage limit. Abort.")

        if self.access_token is None:
            self._refresh_access_token()

        self.num_requests += 1

        response = self.requests_session.request(
            method, url, headers=self._get_auth_header(), params=params, data=data
        )
        if response.ok:
            return response

        error, code = parse_error(response)
        if code == 184:
            self._refresh_access_token()

        LOGGER.info(
            "%s: %s",
            response.status_code,
            response.text,
        )

        raise PardotException(response)

    def _refresh_access_token(self):
        url = "https://login.salesforce.com/services/oauth2/token"
        data = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
        }

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response = self.requests_session.post(url, data=data, headers=headers)
        self.access_token = response.json().get("access_token")

        response = self.requests_session.post(url, data=data, headers=headers)
        data = response.json()

        if response.status_code == 400 and data.get("error") == "invalid_grant":
            raise InvalidCredentials(f"Invalid Credentials: {data}")

        self.access_token = data.get("access_token")
        if not self.access_token:
            LOGGER.warning("failed to refresh token: %s", response.json())
            raise PardotException(response)

    def _fetch(self, method, endpoint, format_params, **kwargs):
        base_formatting = [endpoint, self.api_version]
        if format_params:
            base_formatting.extend(format_params)
        url = (ENDPOINT_BASE + self.get_url).format(*base_formatting)

        params = {"format": "json", **kwargs}

        response = self._make_request(method, url, params)
        _, code = parse_error(response)
        if code == 89:
            # You have requested version 4 of the API, but this account must use version 3
            self.api_version = 3
            url = (ENDPOINT_BASE + self.get_url).format(*base_formatting)
            return self._make_request(method, url, params).json()
        return response.json()

    def get(self, endpoint, format_params=None, **kwargs):
        return self._fetch("get", endpoint, format_params, **kwargs)

    def post(self, endpoint, format_params=None, **kwargs):
        return self._fetch("post", endpoint, format_params, **kwargs)

    def _set_limit(self):
        response = self._make_request(
            "get",
            f"{ENDPOINT_BASE}v5/objects/account?fields=maximumDailyApiCalls,apiCallsUsed",
        )
        if not response.ok:
            self.request_limit = default_limit()

        try:
            data = response.json()
            maximum_calls = data.get("maximumDailyApiCalls", REQUEST_LIMIT)
            used_calls = data.get("apiCallsUsed", 0)
            limit = max(0, maximum_calls - used_calls)
            self.request_limit = int(limit * 0.8)
        except (ValueError, KeyError):
            self.request_limit = default_limit()


def default_limit():
    return int(REQUEST_LIMIT * 0.8)
