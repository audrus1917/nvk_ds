"""
The ``RestAPI`` data resource classes.

``requests`` is the main and single used tool. 
"""

import copy
import json
from urllib.parse import urljoin

import requests
import prefect

from .base import BaseResource, BaseQuery
from .utils import is_abs_url

logger = prefect.context.get("logger")


class RequestError(Exception):
    pass


class RestAPIQuery(BaseQuery):
    """The base class for all REST-API datasources."""    
    default_pagesize = 50
    default_attempts = 1

    def __init__(self, **q_attrs):
        cls = type(self)
        super().__init__(**q_attrs)

        # The sign that it is necessary to fetch data from multiple pages.
        self._x__paged = False

        # The query attributes
        self._x__url = None
        self._x__http_method = "get"
        self._x__headers = {}
        self._x__params = {}
        self._x__data = {}
        self._x__json = {}
        self._x__pagesize = q_attrs.get("pagesize", cls.default_pagesize)
        self._x__limit = 0
        try:
            self._x__attempts = int(q_attrs.get("attemps", cls.default_attempts))
        except (TypeError, ValueError):
            self._x_attempts = cls.default_attempts
        self._result = []
        self._response = None

    def _clone(self):
        """Clone the instance and its attriobutes."""
        c = super()._clone()
        c._x__url = self._x__url
        c._x__http_method = self._x__http_method
        c._x__headers = copy.copy(self._x__headers)
        c._x__params = copy.copy(self._x__params)
        c._x__data = copy.copy(self._x__data)
        c._x__json = copy.copy(self._x__json)
        c._x__pagesize = self._x__pagesize
        c._x__limit = self._x__limit
        c._x__attempts = self._x__attempts
        c._x__paged = self._x__paged
        return c

    @property
    def base_url(self):
        if self._x__ds and hasattr(self._x__ds, "base_url"):
            return self._x__ds.base_url

    def build_url(self):
        return urljoin(self.base_url, self._x__url) \
            if self.base_url and not is_abs_url(self._x__url) else self._x__url

    def get_next_page(self, response_data: dict) -> dict:
        """Returns the next page data."""
        return not self._x__paged

    @staticmethod
    def get_data(response_data):
        return response_data["data"] \
            if isinstance(response_data, dict) and "data" in response_data \
                else response_data

    def execute(self):
        """Executes the query and returns data."""
        cls = type(self)
        if not self._x__ds.opened:
            self._x__ds.open()        
        q_attrs = {}
        if self._x__headers:
            q_attrs["headers"] = self._x__headers
        if self._x__params:
            q_attrs["params"] = self._x__params
        if self._x__data:
            q_attrs["data"] = self._x__data
        if self._x__json:
            q_attrs["json"] = self._x__json
        request_meth = getattr(
            self._x__ds._resource, 
            self._x__http_method
        )
        if request_meth and callable(request_meth):
            response_data = None
            next_page = None
            page_idx = 0
        
            while True:
                if next_page:
                    q_attrs = self.apply_page_data(q_attrs, next_page)
                self._response = request_meth(self.build_url(), **q_attrs)
                if self._response.ok:
                    try:
                        response_data = self._response.json()
                    except json.decoder.JSONDecodeError:
                        break
                    else:
                        self._result.append(cls.get_data(response_data))
                next_page = self.get_next_page(response_data)
                if not next_page:
                    break
                page_idx += 1
                if self._x__limit and page_idx == self._x__limit:
                    break
            
        return self

    def __iter__(self):
        return (item for item in self._result)

    def to_dict(self):
        return dict(
            url=self._x__url,
            params=self._x__params,
            http_method=self._x__http_method,
            data=self._x__data,
            json=self._x__json,
            headers=self._x__headers,

        )


class RestAPIResource(BaseResource):

    query_cls = RestAPIQuery

    def __init__(self, *args, **q_attrs):
        super().__init__(*args, **q_attrs)
        if isinstance(self._config, dict) and "api_url" in self._config:
            self.base_url = self._config["api_url"]

    def open(self):
        self._resource = requests.Session()

    def close(self):
        pass


class IntercomQuery(RestAPIQuery):

    def get_next_page(self, response_data: dict) -> dict:
        """Returns the next page data."""
        if self._x__paged:
            if not response_data:
                return dict(per_page=self._x__pagesize)
            else:
                pages = response_data.get('pages', {})
                pages_next = pages.get('next')
                return dict(
                    per_page=pages.get('per_page', self._x__pagesize),
                    starting_after=pages_next.get('starting_after')
                 ) if pages_next else None

    def apply_page_data(self, q_attrs, next_page):
        _attrs = copy.copy(q_attrs)
        _attrs.setdefault("json", {})["pagination"] = next_page
        return _attrs


class IntercomResource(RestAPIResource):

    query_cls = IntercomQuery

    def __init__(self, *args, **q_attrs):
        super().__init__(*args, **q_attrs)
        access_token = self._config.get("access_token")
        self._headers = {
            "Authorization" : f"Bearer {access_token}",
            "Accept": "application/json",
            "Intercom-Version": "2.0"
        }

    def build_query(self, **q_attrs):
        """Build own query instance."""
        q = super().build_query(**q_attrs)
        if not q._x__headers and self._headers:
            q = q.headers(self._headers)
        return q
