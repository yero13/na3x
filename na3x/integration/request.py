import abc
import json
import logging
import jsonschema
import requests
from jsonschema import validate
from requests.auth import HTTPBasicAuth
from na3x.utils.converter import Types, Converter


class Field:
    FIELD_KEY = 'key'
    FIELD_EXT_ID = 'ext_id'
    FIELD_TYPE = 'type'
    FIELD_SUBITEMS = 'fields'
    FIELD_EXPLICIT = 'explicit'
    FIELD_OPTIONAL = 'optional'
    FIELD_MATCH = 'match'


    @staticmethod
    def is_complex_type(type):
        return type in [Types.TYPE_ARRAY, Types.TYPE_OBJECT]

    @staticmethod
    def is_match(pattern, field):
        if pattern is None:
            return True
        try:
            validate(field, pattern)
            return True
        except jsonschema.ValidationError:
            return False

    @staticmethod
    def parse_field(data, field_cfg, target, is_optional=False):
        field_type = field_cfg[Field.FIELD_TYPE]
        field_key = field_cfg[Field.FIELD_KEY] if Field.FIELD_KEY in field_cfg else None
        field_ext_id = field_cfg[Field.FIELD_EXT_ID] if Field.FIELD_EXT_ID in field_cfg else field_key
        if field_type == Types.TYPE_ARRAY:
            if isinstance(target, dict):  # add to object
                target.update({field_ext_id: []})
                field_value = data[field_key]
            elif not field_key:  # add to array
                field_value = data
            else:
                raise NotImplementedError('Array of arrays is not supported')
            field_pattern = field_cfg[Field.FIELD_MATCH] if Field.FIELD_MATCH in field_cfg else None
            if Field.FIELD_SUBITEMS in field_cfg:
                subfield = next(iter(field_cfg[Field.FIELD_SUBITEMS].values()))  # only one field within array is allowed
                for item in field_value:
                    if Field.is_match(field_pattern, item):
                        Field.parse_field(item, subfield, target[field_ext_id] if field_ext_id else target)
            else:
                target[field_ext_id] = field_value
        elif field_type == Types.TYPE_OBJECT:
            is_explicit = bool(field_cfg[Field.FIELD_EXPLICIT]) if Field.FIELD_EXPLICIT in field_cfg else False
            is_optional = bool(field_cfg[Field.FIELD_OPTIONAL]) if Field.FIELD_OPTIONAL in field_cfg else False
            if field_key:
                try:
                    field_value = data[field_key]
                except KeyError:
                    if is_optional:
                        field_value = None
                    else:
                        raise
            else: # noname object
                field_value = data  # working with the same item
            if is_explicit:
                obj_exp = {}
                if isinstance(target, dict):  # add to object
                    target.update({field_ext_id: obj_exp})
                else:  # add to array
                    target.append(obj_exp)
            for subfield in field_cfg[Field.FIELD_SUBITEMS]:
                Field.parse_field(field_value, field_cfg[Field.FIELD_SUBITEMS][subfield],
                                  obj_exp if is_explicit else target, is_optional)
        else:  # other types
            try:
                field_value = data[field_key]
            except TypeError:
                if is_optional:
                    field_value = None
                else:
                    raise
            casted_value = Converter.convert(field_value, field_type)
            if isinstance(target, dict):  # add to object
                target.update({field_ext_id: casted_value})
            else:  # add to array
                target.append(casted_value)


class Request:
    _CFG_KEY_REQUEST = 'request'
    _CFG_KEY_REQUEST_URL = 'url'
    _CFG_KEY_REQUEST_DATA = 'data'

    def __init__(self, cfg, login, pswd):
        self._logger = logging.getLogger(__class__.__name__)
        self._login = login
        self._pswd = pswd
        self._cfg = cfg
        self._request_cfg = self._cfg[Request._CFG_KEY_REQUEST]

    @abc.abstractmethod
    def result(self):
        return NotImplemented


class ExportRequest(Request):
    TYPE_SET_FIELD_VALUE = 'set_field_value'
    TYPE_CREATE_ENTITY = 'create_entity'
    TYPE_DELETE_ENTITY = 'delete_entity'
    TYPE_CREATE_RELATION = 'create_relation'

    @staticmethod
    def factory(cfg, login, pswd, request_type, mappings=None):
        if request_type == ExportRequest.TYPE_SET_FIELD_VALUE:
            return SetFieldValueRequest(cfg, login, pswd)
        elif request_type == ExportRequest.TYPE_CREATE_ENTITY:
            return CreateEntityRequest(cfg, login, pswd)
        elif request_type == ExportRequest.TYPE_DELETE_ENTITY:
            return DeleteEntityRequest(cfg, login, pswd)
        elif request_type == ExportRequest.TYPE_CREATE_RELATION:
            return CreateRelationRequest(cfg, login, pswd)
        else:
            raise NotImplementedError('Not supported request type - {}'.format(request_type))

    def __init__(self, cfg, login, pswd, mappings=None):
        Request.__init__(self, cfg, login, pswd)
        self.__result = self._perform_request()

    @abc.abstractmethod
    def _perform_request(self):
        return NotImplemented

    @property
    def result(self):
        return self.__result


class CreateEntityRequest(ExportRequest):
    def _perform_request(self):
        request_url = self._request_cfg[Request._CFG_KEY_REQUEST_URL]
        request_data = self._request_cfg[
            Request._CFG_KEY_REQUEST_DATA] if Request._CFG_KEY_REQUEST_DATA in self._request_cfg else None
        self._logger.info('create entity {} on {}'.format(request_data, request_url))
        response = requests.post(request_url,
                                json.dumps(request_data),
                                headers={"Content-Type": "application/json"},
                                auth=HTTPBasicAuth(self._login, self._pswd),
                                verify=True)
        if not response.ok:
            response.raise_for_status()
        return json.loads(response.content, strict=False)


class CreateRelationRequest(ExportRequest):
    def _perform_request(self):
        request_url = self._request_cfg[Request._CFG_KEY_REQUEST_URL]
        request_data = self._request_cfg[
            Request._CFG_KEY_REQUEST_DATA] if Request._CFG_KEY_REQUEST_DATA in self._request_cfg else None
        self._logger.info('create relation {} on {}'.format(request_data, request_url))
        response = requests.post(request_url,
                                json.dumps(request_data),
                                headers={"Content-Type": "application/json"},
                                auth=HTTPBasicAuth(self._login, self._pswd),
                                verify=True)
        if not response.ok:
            response.raise_for_status()


class DeleteEntityRequest(ExportRequest):
    def _perform_request(self):
        request_url = self._request_cfg[Request._CFG_KEY_REQUEST_URL]
        request_data = self._request_cfg[
            Request._CFG_KEY_REQUEST_DATA] if Request._CFG_KEY_REQUEST_DATA in self._request_cfg else None
        self._logger.info('delete {} on {}'.format(request_data, request_url))
        response = requests.delete(request_url,
                                headers={"Content-Type": "application/json"},
                                auth=HTTPBasicAuth(self._login, self._pswd),
                                verify=True)
        if not response.ok:
            response.raise_for_status()


class SetFieldValueRequest(ExportRequest):
    def _perform_request(self):
        request_url = self._request_cfg[Request._CFG_KEY_REQUEST_URL]
        request_data = self._request_cfg[
            Request._CFG_KEY_REQUEST_DATA] if Request._CFG_KEY_REQUEST_DATA in self._request_cfg else None
        self._logger.info('update {} on {}'.format(request_data, request_url))
        response = requests.put(request_url,
                                json.dumps(request_data),
                                headers={"Content-Type": "application/json"},
                                auth=HTTPBasicAuth(self._login, self._pswd),
                                verify=True)
        if not response.ok:
            response.raise_for_status()


class ImportRequest(Request):
    """
    Requests data from Jira. Parses response data accordingly to given rules
    """

    __CFG_KEY_PARAM_TOTAL = 'total'
    __CFG_KEY_PARAM_START_AT = 'startAt'
    __CFG_KEY_PARAM_MAX_RESULTS = 'maxResults'

    __CFG_KEY_RESPONSE = 'response'
    __CFG_KEY_CONTENT_ROOT = 'content-root'

    TYPE_GET_SINGLE_OBJECT = 'single_object'
    TYPE_GET_LIST = 'list'

    @staticmethod
    def factory(cfg, login, pswd, request_type):
        if request_type == ImportRequest.TYPE_GET_LIST:
            return ListImportRequest(cfg, login, pswd)
        elif request_type == ImportRequest.TYPE_GET_SINGLE_OBJECT:
            return SingleObjectImportRequest(cfg, login, pswd)
        else:
            raise NotImplementedError('Not supported request type - {}'.format(request_type))

    def __init__(self, cfg, login, pswd, request_type):
        Request.__init__(self, cfg, login, pswd)
        self._response_cfg = self._cfg[ImportRequest.__CFG_KEY_RESPONSE]
        self._content_root = self._response_cfg[
            ImportRequest.__CFG_KEY_CONTENT_ROOT] if ImportRequest.__CFG_KEY_CONTENT_ROOT in self._response_cfg else None
        self._logger.debug('\n{}'.format(self._response_cfg))
        if request_type == ImportRequest.TYPE_GET_LIST:
            self.__perform_list_request()
        elif request_type == ImportRequest.TYPE_GET_SINGLE_OBJECT:
            self.__perform_single_object_request()
        else:
            raise NotImplementedError('{} - request is not supported'.format(request_type))

    @property
    def result(self):
        return self._get_result()

    @abc.abstractmethod
    def _get_result(self):
        return NotImplemented

    def __perform_single_object_request(self):
        response = self._perform_request()
        self._parse_response(response)

    def __perform_list_request(self):
        while True:
            response = self._perform_request()
            self._parse_response(response if not self._content_root else response[self._content_root])
            if not ImportRequest.__CFG_KEY_PARAM_TOTAL in response:
                break
            total = int(response[ImportRequest.__CFG_KEY_PARAM_TOTAL])
            max_results = int(response[ImportRequest.__CFG_KEY_PARAM_MAX_RESULTS])
            start_at = int(response[ImportRequest.__CFG_KEY_PARAM_START_AT])
            start_at += max_results
            if start_at < total:
                self._request_cfg[Request._CFG_KEY_REQUEST_DATA].update({ImportRequest.__CFG_KEY_PARAM_START_AT: start_at})
                continue
            break

    def __get_request_params(self):
        self.__request_url = self._request_cfg[Request._CFG_KEY_REQUEST_URL]
        self.__request_data = self._request_cfg[
            Request._CFG_KEY_REQUEST_DATA] if Request._CFG_KEY_REQUEST_DATA in self._request_cfg else None

    def _perform_request(self):
        request_url = self._request_cfg[Request._CFG_KEY_REQUEST_URL]
        request_data = self._request_cfg[
            Request._CFG_KEY_REQUEST_DATA] if Request._CFG_KEY_REQUEST_DATA in self._request_cfg else None
        self._logger.info('request {} from {}'.format(request_data, request_url))
        response = requests.get(request_url,
                                request_data,  # for post - json.dumps(self.__request_data),
                                headers={"Content-Type": "application/json"},
                                auth=HTTPBasicAuth(self._login, self._pswd),
                                verify=True)
        if not response.ok:
            response.raise_for_status()
        return json.loads(response.content, strict=False)

    @abc.abstractmethod
    def _parse_response(self, response):
        """
        Parses response into out_data
        :param response: JSON response
        :param out_data: dictionary
        :return: out_data
        """
        return NotImplemented


class SingleObjectImportRequest(ImportRequest):
    def __init__(self, cfg, login, pswd):
        self.__response_values = {}
        ImportRequest.__init__(self, cfg, login, pswd, ImportRequest.TYPE_GET_SINGLE_OBJECT)

    def _parse_response(self, response):
        Field.parse_field(response, self._response_cfg, self.__response_values)

    def _get_result(self):
        return self.__response_values


class ListImportRequest(ImportRequest):
    def __init__(self, cfg, login, pswd, mappings=None):
        self.__response_values = []
        ImportRequest.__init__(self, cfg, login, pswd, ImportRequest.TYPE_GET_LIST)

    def _parse_response(self, response):
        result = []
        if self._content_root:
            Field.parse_field(response, self._response_cfg[self._content_root], result)
        else:
            for field in self._response_cfg: # ToDo: check if several root items is real case
                Field.parse_field(response, self._response_cfg[field], result)
        self.__response_values.extend(result)

    def _get_result(self):
        return self.__response_values
