import json

class EnvVar(object):
    APP_VERSION = "1.0"
    APP_NAME = "Auto Query Expansion"
    API_PREFIX ='/aqe'
    IS_DEBUG = False

    @classmethod
    def deepcopy(cls, x):
        return json.loads(json.dumps(x))



