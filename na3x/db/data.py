import abc
import logging
from na3x.db.connect import MongoDb
from na3x.utils.object import obj_for_name
from na3x.cfg import na3x_cfg, NA3X_TRIGGERS, get_env_params


class CRUD:
    @staticmethod
    def read_single(db, collection, match_params=None):
       return db[collection].find_one(match_params if match_params else {}, {'_id': False})

    @staticmethod
    def read_multi(db, collection, match_params=None):
        return list(db[collection].find(match_params if match_params else {}, {'_id': False}))

    @staticmethod
    def delete_single(db, collection, match_params=None):
        return db[collection].delete_one(match_params).deleted_count

    @staticmethod
    def delete_multi(db, collection, match_params=None):
        return db[collection].delete_many(match_params).deleted_count

    @staticmethod
    def upsert_single(db, collection, object, match_params=None):
        return str(db[collection].update_one(match_params, {"$set": object}, upsert=True).upserted_id)

    @staticmethod
    def upsert_multi(db, collection, object, match_params=None):
        if isinstance(object, list) and len(object) > 0:
            return str(db[collection].insert_many(object).inserted_ids)
        elif isinstance(object, dict):
            return str(db[collection].update_many(match_params, {"$set": object}, upsert=False).upserted_id)


class Trigger:
    ACTION_BEFORE_DELETE = 'before-delete'
    ACTION_AFTER_DELETE = 'after-delete'
    ACTION_BEFORE_UPSERT = 'before-upsert'
    ACTION_AFTER_UPSERT = 'after-upsert'

    @staticmethod
    def factory(db, collection, action):
        triggers_cfg = na3x_cfg[NA3X_TRIGGERS]
        if (collection in triggers_cfg) and (action in triggers_cfg[collection]):
            return obj_for_name(triggers_cfg[collection][action])(db, collection)
        else:
            return None

    def __init__(self, db, collection):
        self._logger = logging.getLogger(__class__.__name__)
        self._db = db
        self._collection = collection

    @abc.abstractmethod
    def execute(self, input_object, match_params):
        return NotImplemented


class AccessParams:
    KEY_DB = 'db'
    KEY_TYPE = 'type'
    TYPE_SINGLE = 'single'
    TYPE_MULTI = 'multi'
    KEY_COLLECTION = 'collection'
    KEY_MATCH_PARAMS = 'match'
    KEY_OBJECT = 'object'
    OPERATOR_OR = '$or'


class Accessor:
    @staticmethod
    def factory(db):
        return Accessor(get_env_params()[db])

    def __init__(self, db):
        self.__db = MongoDb(db).connection
        self.__logger = logging.getLogger(__class__.__name__)

    def __exec_trigger(self, action, collection, input_object, match_params):
        # ToDo: catch exception
        trigger = Trigger.factory(self.__db, collection, action)
        if trigger:
            self.__logger.info('exec trigger {} on {}'.format(action, collection))
            trigger.execute(input_object, match_params)

    def get(self, cfg):
        collection = cfg[AccessParams.KEY_COLLECTION]
        match_params = cfg[AccessParams.KEY_MATCH_PARAMS] if AccessParams.KEY_MATCH_PARAMS in cfg else None

        target_type = cfg[AccessParams.KEY_TYPE] if AccessParams.KEY_TYPE in cfg else AccessParams.TYPE_MULTI
        if target_type == AccessParams.TYPE_SINGLE:
            result = CRUD.read_single(self.__db, collection, match_params)
        elif target_type == AccessParams.TYPE_MULTI:
            result = CRUD.read_multi(self.__db, collection, match_params)
        return result

    def delete(self, cfg, triggers_on=True):
        collection = cfg[AccessParams.KEY_COLLECTION]
        match_params = cfg[AccessParams.KEY_MATCH_PARAMS] if AccessParams.KEY_MATCH_PARAMS in cfg else {}
        target_type = cfg[AccessParams.KEY_TYPE] if AccessParams.KEY_TYPE in cfg else AccessParams.TYPE_MULTI
        if triggers_on:
            self.__exec_trigger(Trigger.ACTION_BEFORE_DELETE, collection, None, match_params)
        if target_type == AccessParams.TYPE_SINGLE:
            result = CRUD.delete_single(self.__db, collection, match_params)
        elif target_type == AccessParams.TYPE_MULTI:
            result = CRUD.delete_multi(self.__db, collection, match_params)
        if triggers_on:
            self.__exec_trigger(Trigger.ACTION_AFTER_DELETE, collection, None, match_params)
        return result

    def upsert(self, cfg, triggers_on=True):
        input_object = cfg[AccessParams.KEY_OBJECT]
        collection = cfg[AccessParams.KEY_COLLECTION]
        match_params = cfg[AccessParams.KEY_MATCH_PARAMS] if AccessParams.KEY_MATCH_PARAMS in cfg else {}
        if triggers_on:
            self.__exec_trigger(Trigger.ACTION_BEFORE_UPSERT, collection, input_object, match_params)
        if cfg[AccessParams.KEY_TYPE] == AccessParams.TYPE_SINGLE:
            result =  CRUD.upsert_single(self.__db, collection, input_object, match_params)
        elif cfg[AccessParams.KEY_TYPE] == AccessParams.TYPE_MULTI:
            result =  CRUD.upsert_multi(self.__db, collection, input_object, match_params)
        if triggers_on:
            self.__exec_trigger(Trigger.ACTION_AFTER_UPSERT, collection, input_object, match_params)
        return result
