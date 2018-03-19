import abc
import logging
from na3x.db.connect import MongoDb
from na3x.utils.object import obj_for_name
from na3x.cfg import na3x_cfg, NA3X_TRIGGERS, get_env_params


class CRUD:
    """
    Wrapper for PyMongo collection-level operations
    """
    @staticmethod
    def read_single(db, collection, match_params=None):
        """
        Wrapper for pymongo.find_one()
        :param db: db connection
        :param collection: collection to read data from
        :param match_params: a query that matches the documents to select
        :return: document ('_id' is excluded from result)
        """
        return db[collection].find_one(match_params if match_params else {}, {'_id': False})

    @staticmethod
    def read_multi(db, collection, match_params=None):
        """
        Wrapper for pymongo.find()
        :param db: db connection
        :param collection: collection to read data from
        :param match_params: a query that matches the documents to select
        :return: list of documents ('_id' is excluded from result)
        """
        return list(db[collection].find(match_params if match_params else {}, {'_id': False}))

    @staticmethod
    def delete_single(db, collection, match_params=None):
        """
        Wrapper for pymongo.delete_one()
        :param db: db connection
        :param collection: collection to update
        :param match_params: a query that matches the documents to delete
        :return: delected count
        """
        return db[collection].delete_one(match_params).deleted_count

    @staticmethod
    def delete_multi(db, collection, match_params=None):
        """
        Wrapper for pymongo.delete_many()
        :param db: db connection
        :param collection: collection to update
        :param match_params: a query that matches the documents to delete
        :return: delected count
        """
        return db[collection].delete_many(match_params).deleted_count

    @staticmethod
    def upsert_single(db, collection, object, match_params=None):
        """
        Wrapper for pymongo.update_one()
        :param db: db connection
        :param collection: collection to update
        :param object: the modifications to apply
        :param match_params: a query that matches the documents to update
        :return: id of updated document
        """
        return str(db[collection].update_one(match_params, {"$set": object}, upsert=True).upserted_id)

    @staticmethod
    def upsert_multi(db, collection, object, match_params=None):
        """
        Wrapper for pymongo.insert_many() and update_many()
        :param db: db connection
        :param collection: collection to update
        :param object: the modifications to apply
        :param match_params: a query that matches the documents to update
        :return: ids of inserted/updated document
        """
        if isinstance(object, list) and len(object) > 0:
            return str(db[collection].insert_many(object).inserted_ids)
        elif isinstance(object, dict):
            return str(db[collection].update_many(match_params, {"$set": object}, upsert=False).upserted_id)


class Trigger:
    """
    Abstract class for triggers
    """
    ACTION_BEFORE_DELETE = 'before-delete'
    ACTION_AFTER_DELETE = 'after-delete'
    ACTION_BEFORE_UPSERT = 'before-upsert'
    ACTION_AFTER_UPSERT = 'after-upsert'

    @staticmethod
    def factory(db, collection, action):
        """
        Instantiate trigger
        :param db: db descriptor
        :param collection: collection to be updated
        :param action: ACTION_BEFORE_DELETE, ACTION_AFTER_DELETE, ACTION_BEFORE_UPSERT, ACTION_AFTER_DELETE
        :return: trigger instance if trigger configured in triggers.json or None
        """
        triggers_cfg = na3x_cfg[NA3X_TRIGGERS]
        if (collection in triggers_cfg) and (action in triggers_cfg[collection]):
            return obj_for_name(triggers_cfg[collection][action])(db, collection)
        else:
            return None

    def __init__(self, db, collection):
        """
        Constructor
        :param db: db descriptor
        :param collection: collection to be updated
        """
        self._logger = logging.getLogger(__class__.__name__)
        self._db = db
        self._collection = collection

    @abc.abstractmethod
    def execute(self, input_object, match_params):
        """
        Abstract method for trigger action implementation. CRUD must be used for accessing MongoDB collections
        :param input_object: see corresponding parameter in Accessor.update method
        :param match_params: see corresponding parameter in Accessor.delete/update method
        """
        return NotImplemented


class AccessParams:
    """
    Configuration parameters for Accessor/CRUD operations
    """
    KEY_DB = 'db'
    KEY_TYPE = 'type'
    TYPE_SINGLE = 'single'
    TYPE_MULTI = 'multi'
    KEY_COLLECTION = 'collection'
    KEY_MATCH_PARAMS = 'match'
    KEY_OBJECT = 'object'
    OPERATOR_OR = '$or'


class Accessor:
    """
    DAO for MongoDB
    """
    @staticmethod
    def factory(db):
        """
        Instantiate Accessor
        :param db: db descriptor in env.json
        :return: Accessor instance for current environment
        """
        return Accessor(get_env_params()[db])

    def __init__(self, db):
        """
        Constructor
        :param db: db descriptor
        """
        self.__db = MongoDb(db).connection
        self.__logger = logging.getLogger(__class__.__name__)

    def __exec_trigger(self, action, collection, input_object, match_params):
        """
        Executes trigger
        :param action: Trigger.ACTION_BEFORE_DELETE, ACTION_AFTER_DELETE, ACTION_BEFORE_UPSERT, ACTION_AFTER_DELETE
        :param collection: see corresponding parameter in delete/update method
        :param input_object: see corresponding parameter in update method
        :param match_params: see corresponding parameter in delete/update method
        """
        trigger = Trigger.factory(self.__db, collection, action)
        if trigger:
            self.__logger.info('exec trigger {} on {}'.format(action, collection))
            # ToDo: catch exception
            trigger.execute(input_object, match_params)

    def get(self, cfg):
        """
        Reads single document or list of documents from MongoDB collection
        :param cfg:
            {
                AccessParams.KEY_COLLECTION: <Collection to read data from>,
                AccessParams.KEY_MATCH_PARAMS: <A query that matches the documents to select>,
                AccessParams.KEY_TYPE: <AccessParams.TYPE_SINGLE or AccessParams.TYPE_MULTI>
            }
        :return: single document or list of documents
        """
        collection = cfg[AccessParams.KEY_COLLECTION]
        match_params = cfg[AccessParams.KEY_MATCH_PARAMS] if AccessParams.KEY_MATCH_PARAMS in cfg else None

        target_type = cfg[AccessParams.KEY_TYPE] if AccessParams.KEY_TYPE in cfg else AccessParams.TYPE_MULTI
        if target_type == AccessParams.TYPE_SINGLE:
            result = CRUD.read_single(self.__db, collection, match_params)
        elif target_type == AccessParams.TYPE_MULTI:
            result = CRUD.read_multi(self.__db, collection, match_params)
        return result

    def delete(self, cfg, triggers_on=True):
        """
        Deletes document(s) from MongoDB collection
        :param cfg:
            {
                AccessParams.KEY_COLLECTION: <Collection to update>,
                AccessParams.KEY_MATCH_PARAMS: <A query that matches the documents to delete>,
                AccessParams.KEY_TYPE: <AccessParams.TYPE_SINGLE or AccessParams.TYPE_MULTI>
            }
        :param triggers_on: enables/disables triggers (default - True)
        :return: result of delete operation - id or number of deleted documents
        """
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
        """
        Updates or inserts single document or list of documents into MongoDB collection
        :param cfg:
            {
                AccessParams.KEY_COLLECTION: <Collection to update>,
                AccessParams.KEY_OBJECT: <The modifications to apply>,
                AccessParams.KEY_MATCH_PARAMS: <A query that matches the documents to update>,
                AccessParams.KEY_TYPE: <AccessParams.TYPE_SINGLE or AccessParams.TYPE_MULTI>
            }
        :param triggers_on: enables/disables triggers (default - True)
        :return: result of upsert operation - id or number of updated documents
        """
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
