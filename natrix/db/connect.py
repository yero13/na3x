from pymongo import MongoClient
import logging
from natrix.cfg import natrix_cfg, NATRIX_DB


class MongoDb:
    __CFG_PARAM_MONGO_DBNAME = 'MONGO_DBNAME'
    __CFG_PARAM_MONGO_HOST = 'MONGO_HOST'
    __CFG_PARAM_MONGO_PORT = 'MONGO_PORT'
    __CFG_PARAM_MONGO_USER = 'MONGO_USER'
    __CFG_PARAM_MONGO_PSWD = 'MONGO_PASSWORD'

    def __init__(self, cfg_db):
        self.__logger = logging.getLogger(__class__.__name__)
        cfg = natrix_cfg[NATRIX_DB][cfg_db]
        self.__connection = MongoClient(
            'mongodb://{}:{}@{}:{:d}/'.format(cfg[MongoDb.__CFG_PARAM_MONGO_USER], cfg[MongoDb.__CFG_PARAM_MONGO_PSWD],
                                              cfg[MongoDb.__CFG_PARAM_MONGO_HOST],
                                              cfg[MongoDb.__CFG_PARAM_MONGO_PORT]))[
            cfg[MongoDb.__CFG_PARAM_MONGO_DBNAME]]
        self.__logger.debug('Mongo {} connection - instantiation'.format(cfg[MongoDb.__CFG_PARAM_MONGO_DBNAME]))

    @property
    def connection(self):
        return self.__connection
