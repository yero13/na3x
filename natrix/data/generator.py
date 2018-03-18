import json
import logging
from natrix.integration.exporter import Exporter
from natrix.integration.importer import Importer
from natrix.transformation.transformer import Transformer
from natrix.utils.cfg import CfgUtils


class Generator():
    __CFG_KEY_STEPS = 'steps'
    __CFG_KEY_STEP_TYPE = 'type'
    __CFG_STEP_TYPE_EXPORT = 'jira.export'
    __CFG_STEP_TYPE_IMPORT = 'jira.import'
    __CFG_STEP_TYPE_TRANSFORMATION = 'db.transformation'
    __CFG_KEY_STEP_CFG = 'cfg'

    def __init__(self, cfg, login, pswd, env_params):
        self.__logger = logging.getLogger(__class__.__name__)
        self.__login = login
        self.__pswd = pswd
        self.__cfg = cfg
        self.__env_cfg = env_params

    def perform(self):
        try:
            for step in self.__cfg[Generator.__CFG_KEY_STEPS]:
                step_type = self.__cfg[Generator.__CFG_KEY_STEPS][step][Generator.__CFG_KEY_STEP_TYPE]
                step_cfg_file = self.__cfg[Generator.__CFG_KEY_STEPS][step][Generator.__CFG_KEY_STEP_CFG]
                self.__logger.info('Perform data generation step: {}, type: {}, configuration: {}'.format(step, step_type, step_cfg_file))
                with open(step_cfg_file) as cfg_file:
                    str_cfg = cfg_file.read()
                step_cfg = json.loads(CfgUtils.substitute_params(str_cfg, self.__env_cfg))
                if step_type == Generator.__CFG_STEP_TYPE_EXPORT:
                    Exporter(step_cfg, self.__login, self.__pswd).perform()
                elif step_type == Generator.__CFG_STEP_TYPE_IMPORT:
                    Importer(step_cfg, self.__login, self.__pswd).perform()
                elif step_type == Generator.__CFG_STEP_TYPE_TRANSFORMATION:
                    Transformer(step_cfg).transform_data()
        except Exception as e:
            logging.error(e, exc_info=True)
