import abc
import logging
from na3x.db.connect import MongoDb


class Integrator():
    _CFG_KEY_DB = 'db'
    _CFG_KEY_REQUESTS = 'requests'
    _CFG_KEY_REQUEST_CFG_FILE = 'cfg'
    _CFG_KEY_REQUEST_TYPE = 'type'
    _CFG_KEY_MAPPING = 'mapping'

    def __init__(self, cfg, login, pswd):
        self._cfg = cfg
        self._login = login
        self._pswd = pswd
        self._logger = logging.getLogger(__class__.__name__)
        self._db = MongoDb(cfg[Integrator._CFG_KEY_DB]).connection
        self._mappings = self._cfg[
            Integrator._CFG_KEY_MAPPING] if Integrator._CFG_KEY_MAPPING in self._cfg else {}

    def perform(self):
        for request in self._cfg[Integrator._CFG_KEY_REQUESTS]:
            request_type = self._cfg[Integrator._CFG_KEY_REQUESTS][request][Integrator._CFG_KEY_REQUEST_TYPE]
            request_cfg_file = self._cfg[Integrator._CFG_KEY_REQUESTS][request][Integrator._CFG_KEY_REQUEST_CFG_FILE]
            self._logger.debug('{}'.format(request_cfg_file))
            self._process_request(request, request_type, request_cfg_file)

    @abc.abstractmethod
    def _process_request(self, request_id, request_type, request_cfg_file):
        return NotImplemented
