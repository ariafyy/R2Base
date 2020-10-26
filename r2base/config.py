import os

class EnvVar(object):
    INDEX_DIR = os.environ.get('INDEX_DIR', '_index')