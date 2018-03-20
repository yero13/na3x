import copy
import json
from na3x.integration.integrator import Integrator
from na3x.integration.request import ExportRequest
from na3x.utils.cfg import CfgUtils


class Exporter(Integrator):
    """
    Performs bulk export according to configuration
	"mapping": {
		"url": "$jira_url" <mappings used in export request, variables should be defined in env.json>
	},
	"requests": {
		"set_search_label": { <request id>
			"cfg": "./cfg/jira/jira-set-issue-field.json", <request configuration file>
			"type": "set_field_value", <request type - Export.TYPE_SET_FIELD_VALUE, TYPE_CREATE_ENTITY, TYPE_DELETE_ENTITY, TYPE_CREATE_RELATION>
			"src.collection": "sprint.assignments_search_labels", <collection with source data for export>
			"static_mapping": { <static mappings applied, variables will be taken from env.json>
				"field": "$field_agilego_search"
			},
			"dynamic_mapping": { <dynamic mappings applied, variables will be taken from src.collection>
				"key": "key",
				"value": "label"
			}
		}
	},
	"db": "$db_jira_export" <db to get exported data>
    """
    __CFG_KEY_SRC_COLLECTION = 'src.collection'
    __CFG_KEY_STATIC_MAPPING = 'static_mapping'
    __CFG_KEY_DYNAMIC_MAPPING = 'dynamic_mapping'
    __CFG_KEY_CALLBACK = 'callback.update_src'

    def _process_request(self, request_id, request_type, request_cfg_file):
        with open(request_cfg_file) as cfg_file:
            str_cfg = cfg_file.read()
        request_cfg = self._cfg[Integrator._CFG_KEY_REQUESTS][request_id]
        src_collection = request_cfg[Exporter.__CFG_KEY_SRC_COLLECTION]
        dataset = list(self._db[src_collection].find({}, {'_id': False}))
        static_mapping = request_cfg[Exporter.__CFG_KEY_STATIC_MAPPING] if Exporter.__CFG_KEY_STATIC_MAPPING in request_cfg else {}
        self._mappings.update(static_mapping)
        dynamic_mapping = request_cfg[Exporter.__CFG_KEY_DYNAMIC_MAPPING]
        for item in dataset:
            item_mappings = {}
            item_request_cfg = copy.deepcopy(str_cfg)
            for mapping_key, item_key in dynamic_mapping.items():
                item_mappings.update({mapping_key: json.dumps(item[item_key]) if isinstance(item[item_key], list) else item[item_key]})
            item_mappings.update(self._mappings)
            itemstrcfg = CfgUtils.substitute_params(item_request_cfg, item_mappings)
            res = ExportRequest.factory(json.loads(itemstrcfg), self._login, self._pswd, request_type).result
            if Exporter.__CFG_KEY_CALLBACK in request_cfg and bool(request_cfg[Exporter.__CFG_KEY_CALLBACK]):
                upd_count = self._db[src_collection].update_one(item, {"$set": res}, upsert=False).modified_count
                self._logger.debug('{} items updated filter: {}, update {}'.format(upd_count, item_mappings, res))
