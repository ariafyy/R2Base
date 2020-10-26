import os
import yaml


class EnvVar(object):
    APP_VERSION = "0.0.1"
    APP_NAME = "R2Base"
    root_dir = os.path.dirname(os.path.realpath(__file__)).replace('r2base', '')

    default = yaml.load(open(os.path.join(root_dir, 'configs/default.yaml'), 'r'))
    INDEX_DIR = os.environ.get('INDEX_DIR', default['INDEX_DIR'])
    MODEL_DIR = os.environ.get("MODEL_DIR", default['MODEL_DIR'])
    API_PREFIX = os.environ.get('API_PREFIX', default['API_PREFIX'])
    IS_DEBUG = bool(os.environ.get('IS_DEBUG', default['IS_DEBUG']))




