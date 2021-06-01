# -*- coding: utf-8 -*-
import os
import json
import operator
import logging
import requests
from . import __name__, __version__
from typing import Optional, Dict, Any
try:
    import urlparse
except ImportError:
    from urllib import parse as urlparse


GET = 'GET'
POST = 'POST'
DELETE = 'DELETE'
PUT = 'PUT'
PATCH = 'PATCH'
REQUEST_TIMEOUT_ENV_VAR = 'PYTHON_DIGITALOCEAN_REQUEST_TIMEOUT_SEC'


class Error(Exception):
    """Base exception class for this module"""
    pass


class TokenError(Error):
    pass


class DataReadError(Error):
    pass


class JSONReadError(Error):
    pass


class NotFoundError(Error):
    pass


class EndPointError(Error):
    pass


class Requester:
    """Requester class encapsulating api interaction"""
    tokens = []
    _last_used = 0
    end_point = "https://api.digitalocean.com/v2/"

    def __init__(self, token: Optional[str] = None, end_point: Optional[str] = None) -> None:
        self.token = token or os.getenv("DIGITALOCEAN_ACCESS_TOKEN", "")
        self.end_point = end_point or os.getenv("DIGITALOCEAN_END_POINT", "https://api.digitalocean.com/v2/")
        self._log = logging.getLogger(__name__)
        self._session = requests.Session()

        parsed_url = urlparse.urlparse(self.end_point)
        if not parsed_url.scheme or not parsed_url.netloc:
            raise EndPointError("Provided end point is not a valid URL. Please use a valid URL")

        if not parsed_url.path:
            self.end_point += '/'

    @classmethod
    def create(cls, config: Dict[str, Any]) -> 'Session':
        """Create requester by consuming relevant parts of the config"""
        arg_names = ["token", "end_point"]
        kwargs = {}
        for arg in arg_names:
            kwargs[arg] = config.pop(arg, None)
        return Requester(**kwargs)

    def __getstate__(self):
        state = self.__dict__.copy()
        # The logger is not pickleable due to using thread.lock
        del state['_log']
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._log = logging.getLogger(__name__)

    def _perform_request(self, url, type=GET, params=None):
        """
            This method will perform the real request,
            in this way we can customize only the "output" of the API call by
            using self.__call_api method.
            This method will return the request object.
        """
        if params is None:
            params = {}

        if not self.token:
            raise TokenError("No token provided. Please use a valid token")

        url = urlparse.urljoin(self.end_point, url)

        # lookup table to find out the appropriate requests method,
        # headers and payload type (json or query parameters)
        identity = lambda x: x
        json_dumps = lambda x: json.dumps(x)
        lookup = {
            GET: (self._session.get, {'Content-type': 'application/json'}, 'params', identity),
            PATCH: (requests.patch, {'Content-type': 'application/json'},
                    'data', json_dumps),
            POST: (requests.post, {'Content-type': 'application/json'}, 'data',
                   json_dumps),
            PUT: (self._session.put, {'Content-type': 'application/json'}, 'data',
                  json_dumps),
            DELETE: (self._session.delete,
                     {'content-type': 'application/json'},
                     'data', json_dumps),
        }

        requests_method, headers, payload, transform = lookup[type]
        agent = "{0}/{1} {2}/{3}".format('python-digitalocean',
                                         __version__,
                                         requests.__name__,
                                         requests.__version__)
        headers.update({'Authorization': 'Bearer ' + self.token,
                        'User-Agent': agent})
        kwargs = {'headers': headers, payload: transform(params)}

        timeout = self.get_timeout()
        if timeout:
            kwargs['timeout'] = timeout

        # remove token from log
        headers_str = str(headers)
        for i, token in enumerate(self.tokens):
            headers_str = headers_str.replace(token.strip(), 'TOKEN%s' % i)
        self._log.debug('%s %s %s:%s %s %s' %
                        (type, url, payload, params, headers_str, timeout))

        return requests_method(url, **kwargs)

    def _deal_with_pagination(self, url, method, params, data):
        """
            Perform multiple calls in order to have a full list of elements
            when the API are "paginated". (content list is divided in more
            than one page)
        """
        all_data = data
        while data.get("links", {}).get("pages", {}).get("next"):
            url, query = data["links"]["pages"]["next"].split("?", 1)

            # Merge the query parameters
            for key, value in urlparse.parse_qs(query).items():
                params[key] = value

            data = self._perform_request(url, method, params).json()

            # Merge the dictionaries
            for key, value in data.items():
                if isinstance(value, list) and key in all_data:
                    all_data[key] += value
                else:
                    all_data[key] = value

        return all_data

    def __init_ratelimit(self, headers):
        # Add the account requests/hour limit
        self.ratelimit_limit = headers.get('Ratelimit-Limit', None)
        # Add the account requests remaining
        self.ratelimit_remaining = headers.get('Ratelimit-Remaining', None)
        # Add the account requests limit reset time
        self.ratelimit_reset = headers.get('Ratelimit-Reset', None)

    @property
    def token(self):
        # use all the tokens round-robin style
        if self.tokens:
            self._last_used = (self._last_used + 1) % len(self.tokens)
            return self.tokens[self._last_used]
        return ""

    @token.setter
    def token(self, token):
        self._last_used = 0
        if isinstance(token, list):
            self.tokens = token
        else:
            # for backward compatibility
            self.tokens = [token]

    def get_timeout(self):
        """
            Checks if any timeout for the requests to DigitalOcean is required.
            To set a timeout, use the REQUEST_TIMEOUT_ENV_VAR environment
            variable.
        """
        timeout_str = os.environ.get(REQUEST_TIMEOUT_ENV_VAR)
        if timeout_str:
            try:
                return float(timeout_str)
            except:
                self._log.error('Failed parsing the request read timeout of '
                                '"%s". Please use a valid float number!' %
                                        timeout_str)
        return None

    def get_data(self, url, type=GET, params=None):
        """
            This method is a basic implementation of __call_api that checks
            errors too. In case of success the method will return True or the
            content of the response to the request.

            Pagination is automatically detected and handled accordingly
        """
        if params is None:
            params = dict()

        # If per_page is not set, make sure it has a sane default
        if type is GET:
            params.setdefault("per_page", 200)

        req = self._perform_request(url, type, params)
        if req.status_code == 204:
            return True

        if req.status_code == 404:
            raise NotFoundError()

        try:
            data = req.json()

        except ValueError as e:
            raise JSONReadError(
                'Read failed from DigitalOcean: %s' % str(e)
            )

        if not req.ok:
            msg = [data[m] for m in ("id", "message") if m in data][1]
            raise DataReadError(msg)

        # init request limits
        self.__init_ratelimit(req.headers)

        # If there are more elements available (total) than the elements per
        # page, try to deal with pagination. Note: Breaking the logic on
        # multiple pages,
        pages = data.get("links", {}).get("pages", {})
        if pages.get("next") and "page" not in params:
            return self._deal_with_pagination(url, type, params, data)
        else:
            return data

    def __str__(self):
        return "<%s>" % self.__class__.__name__

    def __unicode__(self):
        return u"%s" % self.__str__()

    def __repr__(self):
        return str(self)


class BaseAPI(object):
    """
        Basic api class for  
    """

    def __init__(self, *args, requester=None, **kwargs):
        self._log = logging.getLogger(__name__)

        self._requester = requester or Requester.create(kwargs)
        for attr in kwargs.keys():
            setattr(self, attr, kwargs[attr])

    def __getstate__(self):
        state = self.__dict__.copy()
        # The logger is not pickleable due to using thread.lock
        del state['_log']
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._log = logging.getLogger(__name__)

    @property
    def end_point(self):
        return self._requester.end_point

    @property
    def _session(self):
        return self._requester._session

    @_session.setter
    def _session(self, value):
        self._requester._session = value

    @property
    def token(self):
        # use all the tokens round-robin style
        return self._requester.token 

    @token.setter
    def token(self, token):
        self._requester.token = token

    def get_data(self, url, type=GET, params=None):
        """
            This method is a basic implementation of __call_api that checks
            errors too. In case of success the method will return True or the
            content of the response to the request.

            Pagination is automatically detected and handled accordingly
        """
        return self._requester.get_data(url, type, params)

    def __str__(self):
        return "<%s>" % self.__class__.__name__

    def __unicode__(self):
        return u"%s" % self.__str__()

    def __repr__(self):
        return str(self)
