import json


na3x_cfg = {}


NA3X_DB = 'db'
NA3X_TRIGGERS = 'triggers'
NA3X_ENV = 'env'


def init(cfg):
    """
    Initialiaze na3x
    :param cfg: db, triggers, environment variables configuration
    """
    global na3x_cfg
    with open(cfg[NA3X_DB]) as db_cfg_file:
        na3x_cfg[NA3X_DB] = json.load(db_cfg_file, strict=False)
    with open(cfg[NA3X_TRIGGERS]) as triggers_cfg_file:
        na3x_cfg[NA3X_TRIGGERS] = json.load(triggers_cfg_file, strict=False)
    with open(cfg[NA3X_ENV]) as env_cfg_file:
        na3x_cfg[NA3X_ENV] = json.load(env_cfg_file, strict=False)


CFG_ENV_TEST = 'test'
CFG_ENV_PROD = 'prod'
IS_TEST = False

def get_env_params():
    """
    Return environment variables for current environment
    :return: environment variables
    """
    global na3x_cfg
    global IS_TEST
    return na3x_cfg[NA3X_ENV][CFG_ENV_TEST if IS_TEST else CFG_ENV_PROD]
