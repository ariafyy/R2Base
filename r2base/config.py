import os


class EnvVar(object):
    APP_VERSION = "0.0.1"
    APP_NAME = "R2Base"

    INDEX_DIR = os.environ.get('INDEX_DIR', '_index')
    MODEL_DIR = os.environ.get("MODEL_DIR", "resources")
    API_PREFIX = os.environ.get('API_PREFIX', "/r2base")
    IS_DEBUG = bool(os.environ.get('IS_DEBUG', False))




