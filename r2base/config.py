import os
import yaml
import json

class EnvVar(object):
    APP_VERSION = "0.0.1"
    APP_NAME = "R2Base"
    root_dir = os.path.dirname(os.path.realpath(__file__)).replace('r2base', '')

    default = yaml.load(open(os.path.join(root_dir, 'configs/default.yaml'), 'r'), Loader=yaml.FullLoader)
    ES_SETTING = json.load(open(os.path.join(root_dir, 'configs/es_setting.json'), 'r'))

    INDEX_DIR = os.environ.get('INDEX_DIR', default['INDEX_DIR'])
    MODEL_DIR = os.environ.get("MODEL_DIR", default['MODEL_DIR'])
    API_PREFIX = os.environ.get('API_PREFIX', default['API_PREFIX'])
    IS_DEBUG = bool(os.environ.get('IS_DEBUG', default['IS_DEBUG']))
    UMAP_BACKEND = os.environ.get('UMAP_BACKEND', default['UMAP_BACKEND'])

    ES_URL = os.environ.get('ES_URL', default['ES_URL'])
    ES_SHARD_NUM = int(os.environ.get('ES_SHARD_NUM', default['ES_SHARD_NUM']))
    ES_REPLICA_NUM = int(os.environ.get('ES_REPLICA_NUM', default['ES_REPLICA_NUM']))
    ES_SETTING['index']['number_of_shards'] = ES_SHARD_NUM
    ES_SETTING['index']['number_of_replicas'] = ES_REPLICA_NUM

    MAX_NUM_INDEX = int(os.environ.get('MAX_NUM_INDEX', 1000))
    INDEX_BATCH_SIZE = int(os.environ.get('INDEX_BATCH_SIZE', 100))

    REDIS_URL = os.environ.get('REDIS_URL', "redis://:Hzlh2020@localhost:6379/0")

    LRU_CAP = os.environ.get("LRU_CAP", 10)

    S3_SECRET = os.environ.get("S3_SECRET", '')
    S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", '')
    S3_ENGINE = os.environ.get("S3_ENGINE", 's3')

    MRC_URL = os.environ.get("MRC_URL", "http://quad-0.tepper.cmu.edu:30007/v1/query")


    @classmethod
    def deepcopy(cls, x):
        return json.loads(json.dumps(x))


if __name__ == "__main__":
    print(EnvVar.deepcopy(EnvVar.ES_SETTING))


