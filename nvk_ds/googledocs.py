"""The `Googledocs` dataresource classes."""

from google.oauth2 import service_account
from apiclient import discovery

from .base import BaseResource, BaseQuery


class GoogleSheetQuery(BaseQuery):

    def __init__(self):
        super().__init__()
        self._x__spreadsheet_id = ''
        self._x__range = None
        self._result = None

    def _clone(self):
        c = super()._clone()
        c._x__spreadsheet_id = self._x__spreadsheet_id
        c._x__range = self._x__range
        return c

    def to_dict(self):
        return dict(
            spreadsheet_id=self._x__spreadsheet_id,
            range=self._x__range
        )

    def execute(self):
        if not self._x__ds.opened:
            self._x__ds.open()
        q = self._x__ds._resource.spreadsheets().values().get(
            spreadsheetId=self._x__spreadsheet_id, 
            range=self._x__range
        ).execute()
        self._result = q.get('values', [])
        return self

    def __iter__(self):
        return (item for item in self._result)


class GoogleDocResource(BaseResource):

    DEFAULT_SERVICE_NAME = 'sheets'
    DEFAULT_VERSION = 'v4'
    DEFAULT_SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets', 
        'https://www.googleapis.com/auth/drive'
    ]

    query_cls = GoogleSheetQuery

    def open(self):
        cls = type(self)
        credentials = service_account.Credentials\
            .from_service_account_info(self._config)\
            .with_scopes(cls.DEFAULT_SCOPES)
        self._resource = discovery.build(
            cls.DEFAULT_SERVICE_NAME, 
            cls.DEFAULT_VERSION, 
            credentials=credentials, 
            cache_discovery=False
        )

    def close(self):
        pass

    def sheets_codes(self, spreadsheet_id):
        sheets_metadata = self._resource.spreadsheets()\
            .get(spreadsheetId=spreadsheet_id)\
            .execute()
        sheets = sheets_metadata.get('sheets', [])
        for item in sheets:
            yield item.get('properties', {}).get('title', None)
