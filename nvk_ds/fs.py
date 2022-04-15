"""The files data resource classes."""

from pathlib import Path
import json

from .base import BaseResource, BaseQuery


class FSQuery(BaseQuery):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._x__format = "json"
        self._result = []

    def _clone(self):
        c = super()._clone()
        c._x__format = self._x__format
        return c

    def execute(self):
        ds_path = Path(self._x__ds._config)
        ds_files = []
        if ds_path.is_dir():
            for path_object in ds_path.glob("**/*"):
                if (path_object.is_file() 
                        and path_object.suffix == self._x__format):
                    ds_files.append(path_object)
        elif ds_path.is_file():
            ds_files.append(ds_path)

        if self._x__format == "json":
            for ds_file in ds_files:
                with open(ds_file.resolve(), "r") as fh:
                    data = json.load(fh)
                    if isinstance(data, list):
                        for item in data:
                            self._result.append(item)
        return self

    def __iter__(self):
        return (item for item in self._result)

    def to_dict(self):
        return dict(
            path=self._x__path,
            format=self._x__format
        )


class FSResource(BaseResource):

    query_cls = FSQuery

    def open(self):
        pass

    def close(self):
        pass
