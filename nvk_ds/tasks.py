"""
Tasks classes for dataresources queries.
"""

import collections.abc
import uuid
import json

from typing import Union, List, Any
from pathlib import Path
from datetime import datetime

try:
    import prefect
except ImportError:
    raise ImportError("The `prefect` doesn't installed")

from prefect import Task, Flow, task
from prefect.engine.state import State, Success, Failed
from prefect.utilities.collections import DotDict

from .base import BaseResource, BaseQuery
from .utils import load_query_from_file, json_serialize

logger = prefect.context.get("logger")

@task
def is_not_empty(x: List[Any]) -> bool:
    if isinstance(x, (list, tuple)):
        return len(x) > 0
    return False

class DataTask(Task):
    """Task for data processing."""

    @staticmethod
    def backup_data(
        data_items: List[Any],
        backup_folder: str,
        obj_name: str,
        session: DotDict
    ):
        """Backup task result data for current session."""
        if data_items and backup_folder \
                and isinstance(data_items, collections.abc.Sequence):
            backup_folder_path = Path(backup_folder)
            if backup_folder_path.is_dir():
                data_folder = backup_folder_path \
                    / session.session_date.strftime("%Y-%m-%dT%H:%M:%S")\
                    / session.session_id
                if not data_folder.is_dir():
                    data_folder.mkdir(parents=True, exist_ok=True)

                data_file = data_folder / "{0}.json".format(obj_name)
                with open(data_file.resolve(), "w") as fh:
                    json.dump(
                        data_items, 
                        fh, 
                        default=json_serialize
                    )

    @classmethod
    def backup_results(
        cls,
        obj: Union[Task, Flow], 
        old_state: State, 
        new_state: State
    ):
        """Backup results data if this one is iterable."""
        backup_options = prefect.config.get("backup", {})
        backup_results = backup_options.get("results", False)
        backup_folder = backup_options.get("folder", None)
        session = getattr(obj, "session", None)
        if (isinstance(new_state, Success) and backup_results) or \
                isinstance(new_state, Failed) and session:
            cls.backup_data(
                new_state.result, 
                backup_folder,
                obj.name,
                session
            )

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("state_handlers", [])\
            .append(type(self).backup_results)
        super().__init__(*args, **kwargs)


class DataResourceTask(Task):
    """
    Task for executing a DML query against a dataresources.
    """
    def __init__(self, dataresource: BaseResource, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._resource = dataresource


class DataQueryTask(DataResourceTask):
    """
    Task for executing a query against a dataresources.
    """
    query_file: str = None
    query_sql: str = None

    @classmethod
    def load_query(cls, file_format: str = "sql") -> Union[str, dict]:
        """Return the query as content of `cls.query_file`."""
        if Path(cls.query_file).is_file():
            return load_query_from_file(
                cls.query_file,
                file_format=file_format
            )

    def __init__(self, *args, **kwargs):
        cls = type(self)
        super().__init__(*args, **kwargs)
        self._query = cls.load_query() if cls.query_file else \
            cls.query_sql if cls.query_sql else None

    @staticmethod
    def fetch_result(q: BaseQuery) -> List[Any]:
        return list(q)

    def get_query_info(self) -> str:
        info = self.__class__.__name__
        rows = self._query.split("\n")
        if rows:
            first_row = rows[0].strip()
            if first_row[0:2] == '--':
                info = first_row
        return info

    def parse_result(q: BaseQuery) -> Any:
        try:
            return list(q)
        except TypeError:
            return q

    def run(self, session: DotDict = None, **query_data) -> BaseQuery:
        """Build, execute and return the query result."""
        cls = type(self)
        self.session = session
        if "query" not in query_data and self._query:
            query_data.update(dict(query=self._query))
        q = self._resource.build_query(**query_data)
        q.execute()
        result = cls.parse_result(q)
        if isinstance(result, (tuple, list)):
            logger.info(
                "{0}: returned result's list length = {1}"\
                    .format(self.name, len(result))
            )
        return result


class FetchIdMixin:

    @staticmethod
    def parse_result(q: BaseQuery) -> List[int]:
        is_dict = False
        result = []
        for idx, item in enumerate(q):
            if idx == 0:
                is_dict = isinstance(item, dict)
            result.append(item.get("id") if is_dict else item[0])
        return result


class FetchNoneMixin:

    @staticmethod
    def parse_result(q: BaseQuery):
        return None


class CreateSessionTask(Task):
    """The task for session data."""

    def run(self):
        session = DotDict(
            session_id=str(uuid.uuid4()),
            session_date=datetime.utcnow()
        )
        logger.info("Start session: {0}, {1}".format(
            session.session_id, 
            session.session_date
        ))
        return session
