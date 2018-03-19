import logging
from na3x.utils.aggregator import Aggregator
from na3x.utils.object import obj_for_name
from na3x.db.data import Accessor, AccessParams
from na3x.utils.converter import Converter


class Validator:
    """
    Validates input object according configuration of validation checks
    """
    __CFG_KEY_CHECKS = 'checks'

    def __init__(self, cfg):
        self.__cfg = cfg
        self.__logger = logging.getLogger(__class__.__name__)

    def validate(self, obj_to_validate, is_substitute=True):
        """
        Performs validation
        :param obj_to_validate:
        :param is_substitute:
        :return: list of validation messages or None
        """
        res = []
        for check in self.__cfg[Validator.__CFG_KEY_CHECKS]:
            self.__logger.info('Performing {} validation against {}'.format(check, obj_to_validate))
            check_res = Check(self.__cfg[Validator.__CFG_KEY_CHECKS][check]).validate(obj_to_validate, is_substitute)
            if check_res:
                if type(check_res) == dict:
                    res.append(check_res)
                else:
                    res.extend(check_res)
        return None if len(res) == 0 else res


class Check:
    """
    Performs validation check according to configuration
    Configuration example:
		"assignment.expertise.conflict": {
			"constraint": {
				"func": "na3x.validation.validator.extract", <function how to get constraint value>
				"params": {
					"db": "db_scrum_api",
					"match": ["key"],
					"collection": "sprint.backlog",
					"field": "components"
				},
				"default": []
			},
			"to_validate": {
				"func": "na3x.validation.validator.extract", <function how to get validated value>
				"params": {
					"db": "db_scrum_api",
					"match": ["group"],
					"collection": "project.team",
					"field": "components"
				},
				"default": []
			},
			"compare": {
				"func": "na3x.validation.validator.no_intersection", <function how to compare constraint vs validated>
				"violation": { <violation message>
					"severity": "warning",
					"message": "Task requires {} component(s) expertise"
				}
			}
    """
    CFG_KEY_VIOLATION_SEVERITY = 'severity'
    CFG_KEY_VIOLATION_MSG = 'message'
    __CFG_KEY_TO_VALIDATE = 'to_validate'
    __CFG_KEY_CONSTRAINT = 'constraint'
    __CFG_KEY_COMPARE = 'compare'
    __CFG_KEY_VIOLATION = 'violation'
    __CFG_KEY_FUNC = 'func'
    __CFG_KEY_FUNC_PARAMS = 'params'
    __CFG_KEY_DEFAULT = 'default'
    __CFG_KEY_DEFAULT_VALUE = 'value'
    __CFG_KEY_DEFAULT_TYPE = 'type'

    def __init__(self, cfg):
        self.__cfg = cfg
        self.__logger = logging.getLogger(__class__.__name__)

    def __compare(self, to_validate, constraint):
        cfg = self.__cfg[Check.__CFG_KEY_COMPARE]
        func = cfg[Check.__CFG_KEY_FUNC]
        return obj_for_name(func)(to_validate, constraint, cfg[Check.__CFG_KEY_VIOLATION])

    def __get_constraint(self, obj_to_validate):
        cfg = self.__cfg[Check.__CFG_KEY_CONSTRAINT]
        func = cfg[Check.__CFG_KEY_FUNC]
        args = cfg[Check.__CFG_KEY_FUNC_PARAMS] if Check.__CFG_KEY_FUNC_PARAMS in cfg else {}
        res = obj_for_name(func)(obj_to_validate, args)
        if not res:
            return self.__get_default_value(cfg)
        else:
            return res

    def __get_to_validate(self, obj_to_validate):
        cfg = self.__cfg[Check.__CFG_KEY_TO_VALIDATE]
        func = cfg[Check.__CFG_KEY_FUNC]
        params = cfg[Check.__CFG_KEY_FUNC_PARAMS] if Check.__CFG_KEY_FUNC_PARAMS in cfg else {}
        res = obj_for_name(func)(obj_to_validate, params)
        if not res:
            return self.__get_default_value(cfg)
        else:
            return res

    def __get_default_value(self, cfg):
        if isinstance(cfg[Check.__CFG_KEY_DEFAULT], dict):
            return Converter.convert(cfg[Check.__CFG_KEY_DEFAULT][Check.__CFG_KEY_DEFAULT_VALUE],
                                     cfg[Check.__CFG_KEY_DEFAULT][Check.__CFG_KEY_DEFAULT_TYPE])
        else:
            return cfg[Check.__CFG_KEY_DEFAULT]

    def validate(self, input, is_substitute): # ToDo: apply 'no substitute' for initial load validations
        """
        Performs validation
        :param input: object to validate
        :param is_substitute: will be used for what-if in the future
        :return: validation result if violated or None
        """
        constraint = self.__get_constraint(input)
        to_validate = self.__get_to_validate(input)
        return self.__compare(to_validate, constraint)


def getter(func):
    """
    @getter decorator function
    :param func: getter function
    :return: getter function result
    """
    def getter_wrapper(input, params):
        return func(input, **params)
    return getter_wrapper


def comparator(func):
    """
    @comparator decorator function
    :param func: comparator function
    :return: comparator function result
    """
    def comparator_wrapper(to_validate, constraint, violation_cfg):
        return func(to_validate, constraint, violation_cfg)
    return comparator_wrapper


def __extract(input, cfg):
    filter = {}
    filter_params = cfg[AccessParams.KEY_MATCH_PARAMS]
    for param in filter_params:
        filter.update({param: input[param]})
    return Accessor.factory(cfg[AccessParams.KEY_DB]).get(
        {AccessParams.KEY_MATCH_PARAMS: filter,
         AccessParams.KEY_COLLECTION: cfg[AccessParams.KEY_COLLECTION],
         AccessParams.KEY_TYPE: cfg[AccessParams.KEY_TYPE]})


def __substitute(input, dataset, cfg):
    CFG_KEY_FIELD = 'field'

    filter_params = cfg[AccessParams.KEY_MATCH_PARAMS]
    for row in dataset:
        is_substitute_target = True
        for param in filter_params:
            if row[param] != input[param]:
                is_substitute_target = False
                break
        if is_substitute_target:
            row[cfg[CFG_KEY_FIELD]] = input[cfg[CFG_KEY_FIELD]]
            return dataset
    dataset.append(input)
    return dataset


@getter
def const(input, **params):
    """
    Return constant value
    :param input:
    :param params:
    :return:
    """
    PARAM_CONSTANT_VALUE = 'value'

    return params.get(PARAM_CONSTANT_VALUE)


@getter
def return_input(input, **params):
    """
    Returns input value
    :param input:
    :param params:
    :return:
    """
    PARAM_FIELD = 'field'

    return input[params.get(PARAM_FIELD)]

@getter
def extract(input, **params):
    """
    Extract field value from document
    :param input:
    :param params:
    :return:
    """
    EXTRACT_FIELD = 'field'

    extract_params = params
    extract_params.update({AccessParams.KEY_TYPE: AccessParams.TYPE_SINGLE})
    return __extract(input, params)[params.get(EXTRACT_FIELD)]


@getter
def aggregate(input, **params):
    """
    Returns aggregate
    :param input:
    :param params:
    :return:
    """
    PARAM_CFG_EXTRACT = 'extract'
    PARAM_CFG_SUBSTITUTE = 'substitute'
    PARAM_CFG_AGGREGATE = 'aggregate'
    AGGR_FIELD = 'field'
    AGGR_FUNC = 'func'

    extract_params = params.get(PARAM_CFG_EXTRACT)
    extract_params.update({AccessParams.KEY_TYPE: AccessParams.TYPE_MULTI})
    dataset = __extract(input, extract_params)
    if PARAM_CFG_SUBSTITUTE in params:
        dataset = __substitute(input, dataset, params.get(PARAM_CFG_SUBSTITUTE))
    cfg = params.get(PARAM_CFG_AGGREGATE)
    res = Aggregator.agg_single_func(dataset, cfg[AGGR_FIELD], cfg[AGGR_FUNC])
    return res


@comparator
def limit_exceed(to_validate, constraint, violation_cfg):
    """
    Compares 2 values and returns violation message if validated value bigger than constraint
    :param to_validate:
    :param constraint:
    :param violation_cfg:
    :return:
    """
    if to_validate > constraint:
        violation_cfg[Check.CFG_KEY_VIOLATION_MSG] = violation_cfg[Check.CFG_KEY_VIOLATION_MSG].format(constraint)
        return violation_cfg
    else:
        return None


@comparator
def no_intersection(to_validate, constraint, violation_cfg):
    """
    Returns violation message if validated and constraint sets have no intersection
    :param to_validate:
    :param constraint:
    :param violation_cfg:
    :return:
    """
    if len(constraint) == 0 or len(set(constraint).intersection(to_validate)) > 0:
        return None
    else:
        violation_cfg[Check.CFG_KEY_VIOLATION_MSG] = violation_cfg[Check.CFG_KEY_VIOLATION_MSG].format(constraint)
        return violation_cfg
