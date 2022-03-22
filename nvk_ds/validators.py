"""Data values validation."""

import re
from datetime import datetime

import prefect

logger = prefect.context.get("logger")


DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%s",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%s",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%d/%m/%Y %H:%M:%s",
    "%d/%m/%Y",
    "%d.%m.%Y %H:%M:%s",
    "%d.%m.%Y"
)


class ValidationError(Exception):
    """Data validation error."""


def is_url(value: str) -> bool:
    """Return whether or not given value is a valid URL."""
    ip_middle_octet = r"(?:\.(?:1?\d{1,2}|2[0-4]\d|25[0-5]))"
    ip_last_octet = r"(?:\.(?:0|[1-9]\d?|1\d\d|2[0-4]\d|25[0-5]))"

    regex = re.compile(  # noqa: W605
        r"^"
        r"(?:(?:https?|ftp)://)"
        r"(?:[-a-z\u00a1-\uffff0-9._~%!$&'()*+,;=:]+"
        r"(?::[-a-z0-9._~%!$&'()*+,;=:]*)?@)?"
        r"(?:"
        r"(?P<private_ip>"
        r"(?:(?:10|127)" + ip_middle_octet + r"{2}" + ip_last_octet + r")|"
        r"(?:(?:169\.254|192\.168)" + ip_middle_octet + ip_last_octet + r")|"
        r"(?:172\.(?:1[6-9]|2\d|3[0-1])" + ip_middle_octet + ip_last_octet + r"))"
        r"|"
        r"(?P<private_host>"
        r"(?:localhost))"
        r"|"
        r"(?P<public_ip>"
        r"(?:[1-9]\d?|1\d\d|2[01]\d|22[0-3])"
        r"" + ip_middle_octet + r"{2}"
        r"" + ip_last_octet + r")"
        r"|"
        r"\[("
        r"([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|"
        r"([0-9a-fA-F]{1,4}:){1,7}:|"
        r"([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|"
        r"([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|"
        r"([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|"
        r"([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|"
        r"([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|"
        r"[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|"
        r":((:[0-9a-fA-F]{1,4}){1,7}|:)|"
        r"fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|"
        r"::(ffff(:0{1,4}){0,1}:){0,1}"
        r"((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}"
        r"(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|"
        r"([0-9a-fA-F]{1,4}:){1,4}:"
        r"((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}"
        r"(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])"
        r")\]|"
        r"(?:(?:(?:xn--)|[a-z\u00a1-\uffff\U00010000-\U0010ffff0-9]-?)*"
        r"[a-z\u00a1-\uffff\U00010000-\U0010ffff0-9]+)"
        r"(?:\.(?:(?:xn--)|[a-z\u00a1-\uffff\U00010000-\U0010ffff0-9]-?)*"
        r"[a-z\u00a1-\uffff\U00010000-\U0010ffff0-9]+)*"
        r"(?:\.(?:(?:xn--[a-z\u00a1-\uffff\U00010000-\U0010ffff0-9]{2,})|"
        r"[a-z\u00a1-\uffff\U00010000-\U0010ffff]{2,}))"
        r")"
        r"(?::\d{2,5})?"
        r"(?:/[-a-z\u00a1-\uffff\U00010000-\U0010ffff0-9._~%!$&'()*+,;=:@/]*)?"
        r"(?:\?\S*)?"
        r"(?:#\S*)?"
        r"$",
        re.UNICODE | re.IGNORECASE
    )
    pattern = re.compile(regex)
    if pattern.match(value):
        return True
    else:
        raise ValidationError


def is_int(value: str) -> int:
    """Return whether or not given value is integer."""
    try:
        return int(re.sub("\s", "", value))
    except (TypeError, ValueError):
        raise ValidationError


def is_float(value: str) -> int:
    """Return whether or not given value is integer."""
    try:
        return float(re.sub("\s", "", value).replace(",", "."))
    except (TypeError, ValueError):
        raise ValidationError


def is_datetime(value: str) -> datetime:
    for datetime_format in DATETIME_FORMATS:
        try:
            return datetime.strptime(value, datetime_format)
        except ValueError:
            pass
    raise ValidationError
