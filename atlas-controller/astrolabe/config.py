import logging

from http.client import HTTPConnection


# Default CLI option values.
DEFAULT_ATLAS_ORGANIZATION = "MongoDB"
DEFAULT_DBUSERNAME = "atlasuser"
DEFAULT_DBPASSWORD = "mypassword123"
DEFAULT_STATUSPOLLINGTIMEOUT = 600
DEFAULT_STATUSPOLLINGFREQUENCY = 1


# Default values of configuration options.
DEFAULT_BASEURL = "https://cloud.mongodb.com/api/atlas/v1.0/"
DEFAULT_HTTPTIMEOUT = 10


# Environment variables used to determine Atlas project and cluster name.
# Value are set at runtime using Evergreen's default expansions.
PROJECTNAME_ENVVAR = "EVERGREEN_PROJECT_ID"     # use ${project} expansion
CLUSTERNAME_ENVVAR = "ATLAS_CLUSTER_NAME"       # user-defined
CLUSTERNAMESALT_ENVVAR = "EVERGREEN_BUILD_ID"   # use ${build_id} expansion


# Application configuration class.
class AppConfig:
    # Map of configurable option names and the associated environment variables
    # that can be used to set them.
    ENVVARS = {
        "baseurl"       : "ASTROLABE_API_BASE_URL",
        "apiusername"   : "ASTROLABE_API_USERNAME",
        "apipassword"   : "ASTROLABE_API_PASSWORD",
        "httptimeout"   : "ASTROLABE_HTTP_TIMEOUT"
    }

    def __init__(self, **kwargs):
        self.baseurl = DEFAULT_BASEURL
        self.httptimeout = DEFAULT_HTTPTIMEOUT
        self.apipassword = None
        self.apiusername = None
        self.reconfigure(**kwargs)

    def reconfigure(self, **kwargs):
        verbose = kwargs.pop("verbose")
        if verbose:
            self.enable_http_logging(verbose)

        for opt in ("baseurl", "apiusername", "apipassword", "httptimeout"):
            optval = kwargs.pop(opt)
            if optval is not None:
                setattr(self, opt, optval)

    @staticmethod
    def enable_http_logging(loglevel):
        # Logging for HTTP Requests and Responses.
        HTTPConnection.debuglevel = loglevel

        # Python logging levels are 0, 10, 20, 30, 40, 50
        pyloglevel = loglevel * 10
        logging.basicConfig()
        logging.getLogger().setLevel(pyloglevel)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(pyloglevel)
        requests_log.propagate = True
