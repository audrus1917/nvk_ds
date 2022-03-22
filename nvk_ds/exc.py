"""The exceptions used with this tool."""


class ResourceError(Exception):
    """Generic error class."""

    code = None

    def __init__(self, *arg, **kwargs):
        code = kwargs.pop("code", None)
        if code is not None:
            self.code = code
        super().__init__(*arg, **kwargs)
