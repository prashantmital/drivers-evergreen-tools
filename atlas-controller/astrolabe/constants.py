# Map of option names and their default values.
DEFAULT_CONFIGURATION_VALUES = {
    "baseurl"       : "https://cloud.mongodb.com/api/atlas/v1.0/",
    "dbusername"    : "atlasuser",
    "dbpassword"    : "myatlaspassword123",
    "httptimeout"   : 10,
    "atlastimeout"  : 120
}

# Map of configurable option names and the associated environment variables
# that can be used to set them.
ENVIRONMENT_VARIABLE_NAMES = {
    "baseurl"       : "ASTROLABE_API_BASE_URL",
    "apiusername"   : "ASTROLABE_API_USERNAME",
    "apipassword"   : "ASTROLABE_API_PASSWORD",
    "dbusername"    : "ASTROLABE_DB_USERNAME",
    "dbpassword"    : "ASTROLABE_DB_PASSWORD",
    "httptimeout"   : "ASTROLABE_HTTP_TIMEOUT",
    "atlastimeout"  : "ASTROLABE_ATLAS_TIMEOUT"
}
