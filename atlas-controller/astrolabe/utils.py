import click
import json
import logging
from time import monotonic

from http.client import HTTPConnection
from collections import defaultdict, namedtuple


def enable_http_logging(loglevel):
    """Enables logging of all HTTP requests."""
    # Enable logging for HTTP Requests and Responses.
    HTTPConnection.debuglevel = loglevel

    # Python logging levels are 0, 10, 20, 30, 40, 50
    pyloglevel = loglevel * 10
    logging.basicConfig()
    logging.getLogger().setLevel(pyloglevel)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(pyloglevel)
    requests_log.propagate = True


def assert_subset(dict1, dict2):
    """Utility that asserts that `dict2` is a subset of `dict1`, while
    accounting for nested fields."""
    for key, value in dict2.items():
        if key not in dict1:
            raise AssertionError("not a subset")
        if isinstance(value, dict):
            assert_subset(dict1[key], value)
        else:
            assert dict1[key] == value


class Timer:
    """Class to simplify timing operations."""
    def __init__(self):
        self._start = None
        self._end = None

    def reset(self):
        self.__init__()

    def start(self):
        self._start = monotonic()
        self._end = None

    def stop(self):
        self._end = monotonic()

    @property
    def elapsed(self):
        if self._end is None:
            return monotonic() - self._start
        return self._end - self._start


APMTest = namedtuple("APMTest", ["test_name", "cluster_name", "spec"])


class JSONObject(dict):
    """Dictonary with dot-notation read access."""
    def __getattr__(self, name):
        if name in self:
            return self[name]
        raise AttributeError('{} has no property named {}}.'.format(
            self.__class__.__name__, name))


def _nested_defaultdict():
    """An infinitely nested defaultdict type."""
    return defaultdict(_nested_defaultdict)


def _merge_dictionaries(dicts):
    """Utility method to merge a list of dictionaries.
    Last observed value prevails."""
    result = {}
    for d in dicts:
        result.update(d)
    return result


class _JsonDotNotationType(click.ParamType):
    """Custom Click-type for user-friendly JSON input."""
    def convert(self, value, param, ctx):
        # Return None and target type without change.
        if value is None or isinstance(value, dict):
            return value

        # Parse the input (of type path.to.namespace=value).
        ns, config_value = value.split("=")
        ns_path = ns.split(".")
        return_value = _nested_defaultdict()

        # Construct dictionary from parsed option.
        pointer = return_value
        for key in ns_path:
            if key == ns_path[-1]:
                pointer[key] = config_value
            else:
                pointer = pointer[key]

        # Convert nested defaultdict into vanilla dictionary.
        return json.loads(json.dumps(return_value))
