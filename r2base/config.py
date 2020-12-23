import os
import yaml


class EnvVar(object):
    APP_VERSION = "0.0.1"
    APP_NAME = "R2Base"
    root_dir = os.path.dirname(os.path.realpath(__file__)).replace('r2base', '')

    default = yaml.load(open(os.path.join(root_dir, 'configs/default.yaml'), 'r'), Loader=yaml.FullLoader)
    INDEX_DIR = os.environ.get('INDEX_DIR', default['INDEX_DIR'])
    MODEL_DIR = os.environ.get("MODEL_DIR", default['MODEL_DIR'])
    API_PREFIX = os.environ.get('API_PREFIX', default['API_PREFIX'])
    IS_DEBUG = bool(os.environ.get('IS_DEBUG', default['IS_DEBUG']))
    IV_BACKEND = os.environ.get('IV_BACKEND', default['IV_BACKEND'])
    ANN_BACKEND = os.environ.get('ANN_BACKEND', default['ANN_BACKEND'])
    ES_URL = os.environ.get('ES_URL', default['ES_URL'])

    MAX_NUM_INDEX = int(os.environ.get('MAX_NUM_INDEX', 100))
    INDEX_BATCH_SIZE = int(os.environ.get('INDEX_BATCH_SIZE', 100))




