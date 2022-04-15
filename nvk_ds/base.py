"""The data resources base classes."""

from typing import Any
from abc import ABC, abstractmethod


class ProxyAssignee:
    """The class for query appropriate attributes assignment."""

    def __init__(self, instance: Any, attr_name: str):
        self._attr_name = attr_name
        self._instance = instance 

    def __call__(self, *args, **kwargs) -> Any:
        """Assign or update instance attribute value."""
        c = self._instance._clone()
        existed_attr_name = '_x__{}'.format(self._attr_name)
        if hasattr(c, existed_attr_name):
            if args:
                if len(args) == 1:
                    setattr(c, existed_attr_name, args[0])
                else:
                    setattr(c, existed_attr_name, args)
            elif kwargs:
                _attr = getattr(c, existed_attr_name)
                if isinstance(_attr, dict):
                    _attr.update(kwargs)
            return c
        raise AttributeError


class ProxyDescriptor(object):
    """The query attributes descriptor class."""

    def __get__(self, instance, owner):
        return instance if instance else owner()


class BaseQuery(ABC):
    """The base dataresource query class."""

    x = ProxyDescriptor()

    def __init__(self, **kwargs):
        self._x__ds = None
        self._kwargs = kwargs

    def __getattr__(self, name: str) -> Any:
        return ProxyAssignee(self, name)

    def exists(self, attr_name) -> bool:
        return hasattr(self, '_x__{0}'.format(attr_name))

    def _clone(self) -> Any:
        """Return the cloned instance."""

        c = self.__class__()
        c._x__ds = self._x__ds
        return c

    @abstractmethod
    def execute(self):
        """Execute the query."""


class BaseResource(ABC):
    """The base class for different kinds of data resources."""

    query_cls = None

    def __init__(
        self, 
        config: Any, 
        readonly: bool = False, 
        **kwargs
    ):
        self._config = config
        self._kwargs = kwargs
        self._resource = None
        self.readonly = readonly

    @property
    def opened(self):
        """Return if resource has been  opened."""
        return self._resource is not None

    @abstractmethod
    def open(self):
        """Open the data resource."""
        
    @abstractmethod
    def close(self):
        """Close the data resource."""

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()
        
    def __del__(self):
        self.close()

    def build_query(self, **kwargs):
        """Build own query instance."""
        
        cls = type(self)
        q = cls.query_cls().resource(self)
        for name, value in kwargs.items():
            try:
                fn = getattr(q, name)
            except AttributeError:
                pass
            else:
                q = fn(value)
        return q

    def bulk_insert(
        self, 
        mapper, 
        mappings, 
        render_nulls=False, 
        truncate=False, 
        **kwargs
    ):
        raise NotImplementedError
