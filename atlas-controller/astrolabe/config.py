from collections import namedtuple

from requests.auth import HTTPDigestAuth


# Default CLI option values.
DEFAULT_ATLAS_ORGANIZATION = "MongoDB"
DEFAULT_DBUSERNAME = "atlasuser"
DEFAULT_DBPASSWORD = "mypassword123"


# Environment variables used to determine Atlas project and cluster name.
# Value are set at runtime using Evergreen's default expansions.
PROJECTNAME_ENVVAR = "EVERGREEN_PROJECT_ID"     # use ${project} expansion
CLUSTERNAME_ENVVAR = "ATLAS_CLUSTER_NAME"       # user-defined
CLUSTERNAMESALT_ENVVAR = "EVERGREEN_BUILD_ID"   # use ${build_id} expansion


# Convenience class for storing settings related to polling.
_PollerSettings = namedtuple(
    "PollerSettings",
    ["timeout", "frequency"])


# Convenience class for storing application configuration.
_ControllerConfig = namedtuple(
    "AtlasControllerConfiguration",
    ["base_url", "api_version", "auth", "timeout", "polling_settings",
     "verbose"])


def setup_configuration(atlas_base_url, atlas_api_version, atlas_api_username,
                        atlas_api_password, http_timeout, polling_timeout,
                        polling_frequency, verbose):
    config = _ControllerConfig(
        base_url=atlas_base_url,
        api_version=atlas_api_version,
        auth=HTTPDigestAuth(username=atlas_api_username,
                            password=atlas_api_password),
        timeout=http_timeout,
        polling_settings=_PollerSettings(timeout=polling_timeout,
                                         frequency=polling_frequency),
        verbose=verbose)
    return config
