import json
from string import Template
from na3x.integration.integrator import Integrator
from na3x.integration.request import ImportRequest


class Importer(Integrator):
    """
    Performs bulk import according to configuration
	"mapping": { <mappings used in import request, variables should be defined in env.json>
		"url": "$jira_url",
		"project": "$scrum_project",
		"sprint": "$scrum_sprint",
		"field_agilego_tech": "$field_agilego_tech"
	},
	"requests": {
		"components": { <request id>
			"cfg": "./cfg/jira/jira-components-request.json", <request configuration file>
			"type": "list", <request type - ImportRequest.TYPE_GET_SINGLE_OBJECT  or TYPE_GET_LIST>
			"dest": "components" <destination collection to store imported data>
		},
        ...
    "db": "$db_jira_import" <db to store imported data>
    """
    __CFG_KEY_REQUEST_DEST = 'dest'

    def _process_request(self, request_id, request_type, request_cfg_file):
        with open(request_cfg_file) as cfg_file:
            str_cfg = cfg_file.read()
            if len(self._mappings) > 0:
                str_cfg = Template(str_cfg).safe_substitute(self._mappings)
            request_cfg = json.loads(str_cfg)
        request_dest = self._cfg[Integrator._CFG_KEY_REQUESTS][request_id][Importer.__CFG_KEY_REQUEST_DEST]
        self._db[request_dest].drop()
        result = ImportRequest.factory(request_cfg, self._login, self._pswd, request_type).result
        self._logger.debug(result)
        if isinstance(result, dict):
            res = self._db[request_dest].insert_one(result)
            self._logger.info('collection: {} data {} is saved'.format(request_dest, result))
        elif isinstance(result, list):
            if len(result) > 0:
                self._db[request_dest].insert_many([item for item in result])
            self._logger.info('collection: {} {:d} items are saved'.format(request_dest, len(result)))
        else:
            raise NotImplementedError('{} - request is not supported'.format(request_type))
