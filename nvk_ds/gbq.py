"""The `GBQ` data resource classes."""

import copy
import uuid

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import BadRequest
from google.oauth2 import service_account

from .base import BaseResource, BaseQuery
from .utils import to_stream_gqb


class GBQQuery(BaseQuery):
    
    def __init__(self):
        super().__init__()
        self._x__table_name = ''
        self._x__query = None
        self._x__params = []
        self._result = None
        self._job = None

    def _clone(self):
        c = super()._clone()
        c._x__table_name = self._x__table_name
        c._x__query = self._x__query
        c._x__params = copy.copy(self._x__params)
        return c

    def execute(self):
        if not self._x__resource.opened:
            self._x__resource.open()
        query_kwargs = {}
        if self._x__params:
            query_kwargs['job_config'] = bigquery.QueryJobConfig(
                query_parameters=self._x__params
            )
        query_sql = str(self._x__query).format(table_name=self._x__table_name) \
            if self._x__table_name else self._x__query     
        self._job = self._x__resource._resource.query(
            query_sql, 
            **query_kwargs    
        )
        self._result = [dict(row.items()) for row in self._job]
        return self

    def __iter__(self):
        return (item for item in self._result)

    def to_dict(self):
        return dict(
            query=self._x__query,
            params=self._x__params,
            table_name=self._x__table_name
        )


class GBQResource(BaseResource):

    timeout = 60
    query_cls = GBQQuery

    def open(self):
        credentials = service_account.Credentials\
            .from_service_account_info(self._config)
        self._resource = bigquery.Client(
            project=self._config['project_id'],
            credentials=credentials
        )

    def close(self):
        pass

    def bulk_insert(
        self, 
        mapper, 
        mappings, 
        truncate: bool = False,
        source_format: str = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        **kwargs
    ):
        cls = type(self)
        if not self.opened:
            self.open()

        parquet_schema = kwargs.get('parquet_schema')
        if not parquet_schema:
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        schema = kwargs.get('schema')
        config_params = {'schema': schema} if schema else {'autodetect': True}
        data_stream = to_stream_gqb(
            mappings, 
            source_format=source_format,
            columns=schema or [],
            parquet_schema=parquet_schema
        )
        data_stream.seek(0)
        job_config = bigquery.LoadJobConfig(
            source_format=source_format,
            write_disposition='WRITE_TRUNCATE' if truncate else 'WRITE_APPEND',
            **config_params
        )
        job = self._resource.load_table_from_file(
            data_stream, 
            mapper, 
            job_config=job_config
        )
        try:
            job.result(timeout=cls.timeout)
        except BadRequest:
            raise
        else:
            return job.output_rows
