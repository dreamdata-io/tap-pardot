import backoff
import requests
import singer

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

LOGGER = singer.get_logger()

AUTH_URL = "https://pi.pardot.com/api/login/version/3"
ENDPOINT_BASE = "https://pi.pardot.com/api/"


class PardotException(Exception):
    def __init__(self, message, response_content):
        self.code = response_content.get("@attributes", {}).get("err_code")
        self.response = response_content
        super().__init__(message)


class Client:
    api_version = None
    access_token = None
    refresh_token = None
    client_id = None
    client_secret = None
    business_unit_id = None

    get_url = "{}/version/{}/do/query"
    describe_url = "{}/version/{}/do/describe"

    def __init__(self, business_unit_id, client_id, client_secret, refresh_token, access_token="dummy"):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.business_unit_id = business_unit_id
        self.requests_session = requests.Session()

    def _get_auth_header(self):
        return {
            "Authorization": "Bearer " + self.sftoken, 
            "Pardot-Business-Unit-Id": self.business_unit_id
        }

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.Timeout, requests.exceptions.ConnectionError, PardotException),
        jitter=None,
        max_tries=10,
    )
    def _make_request(self, method, url, params=None, data=None, activity=None):
        LOGGER.info(
            "%s - Making request to %s endpoint %s, with params %s",
            url,
            method.upper(),
            url,
            params,
        )
        
        response = self.requests_session.request(
            method, url, headers=self._get_auth_header(), params=params, data=data
        )
        content = response.json()
        error_json = content.get("err", None) or {}
        error_code = error_json.get("@attributes", {}).get("err_code")
        if error_code == 184:
            # https://developer.pardot.com/kb/error-codes-messages/#error-code-184
            LOGGER.info("Access_token is invalid, unknown, or malformed -- refreshing token once")
            self._refresh_access_token()
            LOGGER.info("Token refresh success")
            response = self.requests_session.request(
                method, url, headers=self._get_auth_header(), params=params
            )
            content = response.json()
        elif error_json:
            activity = activity or f"Making {method} request to {url}"
            raise PardotException(
                "Pardot returned error code {} while {}. Message: {}".format(
                    error_code, activity, error_json
                ),
                content,
            )
        response.raise_for_status()
        return content

    def _refresh_access_token(self):
        url = "https://login.salesforce.com/services/oauth2/token"
        data = {"grant_type": "refresh_token",
                "client_id": self.sf_consumer_key,
                "client_secret": self.sf_consumer_secret,
                "refresh_token": self.sftoken_refresh}
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        response = self.requests_session.post(url, data=data, headers=headers).json()
        self.sftoken = response.get("access_token")
        if not self.sftoken:
            raise Exception(f"Failed to refresh token, status:{response.status_code}, content: {response.text}")

    def describe(self, endpoint, **kwargs):
        url = (ENDPOINT_BASE + self.describe_url).format(endpoint, self.api_version)

        params = {"format": "json", "output": "bulk", **kwargs}

        return self._make_request("get", url, params)

    def _fetch(self, method, endpoint, format_params, **kwargs):
        base_formatting = [endpoint, self.api_version]
        if format_params:
            base_formatting.extend(format_params)
        url = (ENDPOINT_BASE + self.get_url).format(*base_formatting)

        params = {"format": "json", "output": "bulk", **kwargs}

        return self._make_request(method, url, params)

    def get(self, endpoint, format_params=None, **kwargs):
        return self._fetch("get", endpoint, format_params, **kwargs)

    def post(self, endpoint, format_params=None, **kwargs):
        return self._fetch("post", endpoint, format_params, **kwargs)
