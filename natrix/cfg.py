import json


natrix_cfg = {}


NATRIX_DB = 'db'
NATRIX_TRIGGERS = 'triggers'
NATRIX_ENV = 'env'


def init(cfg):
    global natrix_cfg
    with open(cfg[NATRIX_DB]) as db_cfg_file:
        natrix_cfg[NATRIX_DB] = json.load(db_cfg_file, strict=False)
    with open(cfg[NATRIX_TRIGGERS]) as triggers_cfg_file:
        natrix_cfg[NATRIX_TRIGGERS] = json.load(triggers_cfg_file, strict=False)
    with open(cfg[NATRIX_ENV]) as env_cfg_file:
        natrix_cfg[NATRIX_ENV] = json.load(env_cfg_file, strict=False)


CFG_ENV_TEST = 'test'
CFG_ENV_PROD = 'prod'
IS_TEST = False

def get_env_params():
    global natrix_cfg
    global IS_TEST
    return natrix_cfg[NATRIX_ENV][CFG_ENV_TEST if IS_TEST else CFG_ENV_PROD]
