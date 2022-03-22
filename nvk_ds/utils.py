"""The utilities for this module."""

from typing import Iterable, List, Any

import os
import io
import json
import uuid
import argparse

import pandas as pd

from datetime import datetime, date
from urllib.parse import urlparse
from functools import wraps

import pyarrow as pa
import pyarrow.parquet as pq

from .validators import is_url, DATETIME_FORMATS, ValidationError


def json_serialize(obj: Any) -> str:
    """JSON serializer for objects not serializable by default json."""
    if isinstance(obj, (datetime, date)):
        return obj.strftime('%Y-%m-%d %H:%M:%S UTC')
    if issubclass(type(obj), datetime) and hasattr(obj, 'strftime'):
        return obj.strftime('%Y-%m-%d %H:%M:%S UTC')
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError ("Type %s not serializable" % type(obj))


def load_query_from_file(query_file: str, file_format: str = "sql") -> Any:
    with open(query_file, 'r') as fh:
        query = fh.read()
        if file_format == "json":
            try:
                return json.loads(query)
            except json.decoder.JSONDecodeError:
                pass
        else:
            return query


def cached_data(json_file):
    """The file based json-like data cache decorator."""

    def decorator(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            if os.path.exists(json_file):
                with open(json_file, 'r') as fh:
                    data = json.load(fh)
            else:
                data = fn(*args, **kwargs)
                with open(json_file, 'w') as fh:
                    json.dump(data, fh, default=json_serialize)
            return data
        return wrapped
    return decorator


def is_abs_url(url):
    try:
        if is_url(url):
            parsed_url = urlparse(url)
            return parsed_url.scheme and parsed_url.netloc
    except ValidationError:
        return False

def to_stream_gqb(
    items: List[Any], 
    source_format: str = "NEWLINE_DELIMITED_JSON", 
    columns: Iterable = None,
    parquet_schema: List[Any] = None
) -> io.StringIO:
    if source_format == "NEWLINE_DELIMITED_JSON":
        stream = io.StringIO()
        for item in items:
            json.dump(item, stream, default=json_serialize)
            stream.write('\n')
    elif source_format == "PARQUET":
        stream = io.BytesIO()
        columns = [getattr(c, "name", str(c))  for c in columns]
        if not columns and isinstance(items, list) and items:
            columns = items[0].keys()
        df = pd.DataFrame(
            [[item.get(name) for name in columns] for item in items],
            columns=columns
        )
        pa_table = pa.Table.from_pandas(df, schema=parquet_schema)
        buf = pa.BufferOutputStream()        
        pq.write_table(pa_table, buf)
        stream.write(buf.getvalue())
    stream.seek(0)
    return stream


def fromisoformat(value, raise_exc=True):

    for datetime_format in DATETIME_FORMATS:
        try:
            return datetime.strptime(value, datetime_format)
        except ValueError:
            pass
    if raise_exc:
        raise argparse.ArgumentTypeError(
            "Invalid format: {0}".format(value)
        )
