"""
The PostgreSQL dataresource and query classes.
"""

import copy
import psycopg2
import psycopg2.extras

from .base import BaseResource, BaseQuery


class PgSQLQuery(BaseQuery):
    
    def __init__(self):
        super().__init__()
        self._x__table_name = ''
        self._x__query = ''
        self._x__params = {}
        self._result = []

    def _clone(self):
        c = super()._clone()
        c._x__table_name = self._x__table_name
        c._x__query = self._x__query
        c._x__params = copy.copy(self._x__params)
        return c

    def execute(self):
        if not self._x__ds.opened:
            self._x__ds.open()
        q = self._x__ds._resource.cursor(
            cursor_factory=psycopg2.extras.DictCursor
        )

        query_sql = str(self._x__query).format(table_name=self._x__table_name) \
            if self._x__table_name else self._x__query
        
        q.execute(query_sql, self._x__params)

        if not q.description:
            self._result = None
        else:
            self._result = q.fetchall()
        return self

    def __iter__(self):
        return (item for item in self._result)

    def to_dict(self):
        return dict(
            query=self._x__query,
            params=self._x__params
        )


class PgSQLResource(BaseResource):

    query_cls = PgSQLQuery

    def open(self):
        self._resource = psycopg2.connect(self._config)

    def close(self):
        if self.opened:
            self._resource.close()

    def get_cursor(self):
        return self._resource.cursor()

    def commit(self):
        self._resource.commit()

    def rollback(self):
        self._resource.rollback()
