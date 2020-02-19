from collections import namedtuple
from enum import Enum

from requests.auth import HTTPDigestAuth

from astrolabe.utils import JSONObject


_CONFIG_DEFAULTS = {
    "ATLAS_ORGANIZATION"    : "MongoDB",
    "DB_USERNAME"           : "atlasuser",
    "DB_PASSWORD"           : "mypassword123"
}


CONFIG_DEFAULTS = JSONObject(_CONFIG_DEFAULTS)


_CONFIG_ENVVARS = {
    "PROJECT_NAME"      : "EVERGREEN_PROJECT_ID",   # ${project} in EVG
    "CLUSTER_NAME_SALT" : "EVERGREEN_BUILD_ID"      # ${build_id} in EVG
}


CONFIG_ENVVARS  = JSONObject(_CONFIG_ENVVARS)


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
