import abc
import logging
import re
import pandas as pd
from na3x.utils.object import obj_for_name
from na3x.db.data import Accessor, AccessParams
from na3x.utils.converter import Converter


class Transformer():
    __CFG_KEY_TRANSFORMATION_SETS = 'transformation-sets'

    def __init__(self, cfg):
        self.__cfg = cfg
        self.__logger = logging.getLogger(__class__.__name__)

    def transform_data(self):
        for transform_set in self.__cfg[Transformer.__CFG_KEY_TRANSFORMATION_SETS]:
            self.__logger.info('Processing transformation set {}'.format(transform_set))
            TransformationSet(self.__cfg[Transformer.__CFG_KEY_TRANSFORMATION_SETS][transform_set]).perform()


class TransformationSet:
    __CFG_KEY_DB = 'db'
    __CFG_KEY_SRC_DB = 'src.db'
    __CFG_KEY_DEST_DB = 'dest.db'
    __CFG_KEY_TRANSFORMATIONS = 'transformations'

    def __init__(self, cfg):
        self.__cfg = cfg
        self.__logger = logging.getLogger(__class__.__name__)
        self.__src_db = self.__cfg[TransformationSet.__CFG_KEY_DB][TransformationSet.__CFG_KEY_SRC_DB]
        self.__dest_db = self.__cfg[TransformationSet.__CFG_KEY_DB][TransformationSet.__CFG_KEY_DEST_DB]

    def perform(self):
        for transformation in self.__cfg[TransformationSet.__CFG_KEY_TRANSFORMATIONS]:
            self.__logger.info('Processing transformation {}'.format(transformation))
            Transformation.factory(self.__cfg[TransformationSet.__CFG_KEY_TRANSFORMATIONS][transformation],
                                   self.__src_db, self.__dest_db).perform(
                self.__cfg[TransformationSet.__CFG_KEY_TRANSFORMATIONS][transformation][
                    Transformation.CFG_KEY_TRANSFORMATION_CFG])


class Transformation:
    """
    Base class for transformations
    """
    __CFG_KEY_TRANSFORMATION = 'transformation'
    __CFG_KEY_TRANSFORMATION_CLASS = 'class'
    CFG_KEY_TRANSFORMATION_CFG = 'cfg'
    __CFG_KEY_LOAD = 'src.db.load'
    _CFG_KEY_LOAD_SRC = 'src'
    __CFG_KEY_TRANSFORM = 'transform'
    __CFG_KEY_CLEANUP = 'dest.db.cleanup'
    _CFG_KEY_CLEANUP_TARGET = 'target'
    __CFG_KEY_SAVE = 'dest.db.save'
    _CFG_KEY_SAVE_DEST = 'dest'
    _CFG_KEY_FUNC = 'func'
    _CFG_KEY_FUNC_PARAMS = 'params'

    @staticmethod
    def factory(cfg, src_db, dest_db):
        """
        Instantiate Transformation
        :param cfg: transformation configuration
			"class": "na3x.transformation.transformer.Col2XTransformation", <Transformation class>
				"cfg": {
					"src.db.load": {
						"src": "sprint.backlog_links" <Collection(s) to be loaded>
					},
					"transform": {
						"func": "ext.transformers.gantt_links" <transformer function>
					},
					"dest.db.cleanup": {
						"target": "baseline.gantt_links" <Collection to be cleaned during transformation (usually the same as destination)>
					},
					"dest.db.save": {
						"dest": "baseline.gantt_links" <Destination collection>
					}
				}
        :param src_db: source db for transformation
        :param dest_db: destination db for transformation
        :return: Transformation instance
        """
        return obj_for_name(cfg[Transformation.__CFG_KEY_TRANSFORMATION_CLASS])(
            cfg[Transformation.CFG_KEY_TRANSFORMATION_CFG], src_db, dest_db)

    def __init__(self, cfg, src_db, dest_db):
        """
        Constructor
        :param cfg: transformation configuration
        :param src_db: source db for transformation
        :param dest_db: destination db for transformation
        """
        self.__cfg = cfg
        self._logger = logging.getLogger(__class__.__name__)
        self._src_db = src_db
        self._dest_db = dest_db
        self._transformation = self.__cfg[
            Transformation.__CFG_KEY_TRANSFORMATION] if Transformation.__CFG_KEY_TRANSFORMATION in self.__cfg else None

    def __cleanup(self, cfg):
        Accessor.factory(self._dest_db).delete(
            {AccessParams.KEY_COLLECTION: cfg[Transformation._CFG_KEY_CLEANUP_TARGET],
             AccessParams.KEY_TYPE: AccessParams.TYPE_MULTI,
             AccessParams.KEY_MATCH_PARAMS: {}}, triggers_on=False)

    @abc.abstractmethod
    def _load(self, cfg):
        """
        Loads data for transformation
        :param cfg: transformation configuration
        """
        return NotImplemented

    def __save(self, cfg):
        Accessor.factory(self._dest_db).upsert(
            {AccessParams.KEY_COLLECTION: cfg[Transformation._CFG_KEY_SAVE_DEST],
             AccessParams.KEY_TYPE: AccessParams.TYPE_SINGLE if isinstance(self.__res, dict) else AccessParams.TYPE_MULTI,
             AccessParams.KEY_OBJECT: self.__res}, triggers_on=False)

    def __transform(self, cfg):
        func = cfg[Transformation._CFG_KEY_FUNC]
        args = cfg[Transformation._CFG_KEY_FUNC_PARAMS] if Transformation._CFG_KEY_FUNC_PARAMS in cfg else {}
        self.__res = obj_for_name(func)(self.__src, args)

    def perform(self, cfg):
        """
        Performs transformation according to configuration
        :param cfg: transformation configuration
        """
        self.__src = self._load(cfg[Transformation.__CFG_KEY_LOAD])
        self.__transform(cfg[Transformation.__CFG_KEY_TRANSFORM])
        self.__cleanup(cfg[Transformation.__CFG_KEY_CLEANUP])
        self.__save(cfg[Transformation.__CFG_KEY_SAVE])


class Doc2XTransformation(Transformation):
    """
    Transformation class with single document (object) source
    """
    def _load(self, cfg):
        return Accessor.factory(self._src_db).get(
            {AccessParams.KEY_COLLECTION: cfg[Transformation._CFG_KEY_LOAD_SRC],
             AccessParams.KEY_TYPE: AccessParams.TYPE_SINGLE})


class Col2XTransformation(Transformation):
    """
    Transformation class with single collection source
    """
    def _load(self, cfg):
        return Accessor.factory(self._src_db).get(
            {AccessParams.KEY_COLLECTION: cfg[Transformation._CFG_KEY_LOAD_SRC],
             AccessParams.KEY_TYPE: AccessParams.TYPE_MULTI})


class MultiCol2XTransformation(Transformation):
    """
    Transformation class with multiple collections source
    """
    def _load(self, cfg):
        sources = cfg[Transformation._CFG_KEY_LOAD_SRC]
        src_data = {}
        for collection in sources:
            src_data[collection] = Accessor.factory(self._src_db).get(
                {AccessParams.KEY_COLLECTION: collection, AccessParams.KEY_TYPE: AccessParams.TYPE_MULTI})
        return src_data


class MultiDoc2XTransformation(Transformation):
    """
    Transformation class with multiple documents (objects) source
    """
    def _load(self, cfg):
        sources = cfg[Transformation._CFG_KEY_LOAD_SRC]
        src_data = {}
        for collection in sources:
            src_data[collection] = Accessor.factory(self._src_db).get(
                {AccessParams.KEY_COLLECTION: collection, AccessParams.KEY_TYPE: AccessParams.TYPE_SINGLE})
        return src_data


class MultiColDoc2XTransformation(Transformation):
    """
    Transformation class with multiple documents and collections source
    """
    _CFG_KEY_LOAD_SRC_COLS = 'src.cols'
    _CFG_KEY_LOAD_SRC_DOCS = 'src.docs'

    def _load(self, cfg):
        src_data = {}
        for collection in cfg[MultiColDoc2XTransformation._CFG_KEY_LOAD_SRC_COLS]:
            src_data[collection] = Accessor.factory(self._src_db).get(
                {AccessParams.KEY_COLLECTION: collection, AccessParams.KEY_TYPE: AccessParams.TYPE_MULTI})
        for collection in cfg[MultiColDoc2XTransformation._CFG_KEY_LOAD_SRC_DOCS]:
            src_data[collection] = Accessor.factory(self._src_db).get(
                {AccessParams.KEY_COLLECTION: collection, AccessParams.KEY_TYPE: AccessParams.TYPE_SINGLE})
        return src_data


def transformer(func):
    """
    @transformer decorator function
    :param func: transformer function
    :return: transformer function result
    """
    def transformer_wrapper(input, params):
        return func(input, **params)
    return transformer_wrapper


@transformer
def group_singles2array(input, **params):
    """
    Creates array of strings or ints from objects' fields
    :param input: list of objects
    :param params:
    :return: list
    """
    PARAM_FIELD_KEY = 'field.key'
    PARAM_FIELD_ARRAY = 'field.array'
    PARAM_FIELD_SINGLE = 'field.single'

    field_key = params.get(PARAM_FIELD_KEY) if PARAM_FIELD_KEY in params else None
    field_array = params.get(PARAM_FIELD_ARRAY)
    field_single = params.get(PARAM_FIELD_SINGLE)

    if not field_key:
        res = []
        for item in input:
            res.append(item[field_single])
        return {field_array: res}
    else:
        tdict = {}
        for row in input:
            if not row[field_key] in tdict:
                tdict.update({row[field_key]: [row[field_single]]})
            else:
                tdict[row[field_key]].append(row[field_single])
        res = []
        for key, value in tdict.items():
            res.append({field_key: key, field_array: value})
        return res


@transformer
def ungroup_array2singles(input, **params):
    """
    Creates list of objects from array of singles
    :param input: list of strings or ints
    :param params:
    :return:
    """
    PARAM_FIELD_KEY = 'field.key'
    PARAM_FIELD_ARRAY = 'field.array'
    PARAM_FIELD_SINGLE = 'field.single'

    res = []
    field_key = params.get(PARAM_FIELD_KEY) if PARAM_FIELD_KEY in params else None
    field_array = params.get(PARAM_FIELD_ARRAY)
    field_single = params.get(PARAM_FIELD_SINGLE)
    for row in input:
        if row[field_array] and len(row[field_array]) > 0:
            for value in row[field_array]:
                res.append({field_single: value} if not field_key else {field_key: row[field_key], field_single: value})
    return res


@transformer
def filter_set(input, **params):
    """
    Apply WHERE filter to input dataset
    :param input:
    :param params:
    :return: filtered data
    """
    PARAM_WHERE = 'where'

    return Converter.df2list(pd.DataFrame.from_records(input).query(params.get(PARAM_WHERE)))


@transformer
def sort_set(input, **params):
    """
    Apply sorting to input dataset
    :param input:
    :param params:
    :return: sorted data
    """
    PARAM_SORT_FIELD = 'sort.field'
    PARAM_SORT_ORDER = 'sort.order'

    df = pd.DataFrame.from_records(input)
    sort_field = params.get(PARAM_SORT_FIELD)
    sort_order = params.get(PARAM_SORT_ORDER) if PARAM_SORT_ORDER in params else None
    if sort_order:
        df[sort_field] = df[sort_field].astype('category')
        df[sort_field].cat.set_categories(sort_order, inplace=True)
    df.sort_values(by=sort_field, inplace=True)
    return Converter.df2list(df)


@transformer
def copy(input, **params):
    """
    Copies input or input's selected fields
    :param input:
    :param params:
    :return: input
    """
    PARAM_FIELDS = 'fields'

    def filter_fields(obj, fields):
        return {k:v for k,v in obj.items() if k in fields}

    if PARAM_FIELDS in params:
        fields = params.get(PARAM_FIELDS)
        if isinstance(input, list):
            res = []
            for row in input:
                res.append(filter_fields(row, fields))
            return res
        elif isinstance(input, dict):
            return filter_fields(input, fields)
        else:
            raise NotImplementedError('{} is not supported'.format(type(input)))
    else:
        return input


@transformer
def regexp(input, **params):
    """
    Parses input according to pattern
    :param input:
    :param params:
    :return:
    """
    PARAM_FIELD_TO_PARSE = 'input.field'
    PARAM_PATTERN = 'pattern'
    PARAM_OUTPUT = 'output'
    OUT_DESC_FIELD = 'field'
    OUT_DESC_IDX = 'idx'
    OUT_DESC_TYPE = 'type'

    res = []
    regex = re.compile(params.get(PARAM_PATTERN))
    field2parse = params.get(PARAM_FIELD_TO_PARSE)
    out_desc = params.get(PARAM_OUTPUT)
    for row in input:
        matches = regex.findall(row[field2parse])[0]
        obj = {}
        for desc in out_desc:
            obj[desc[OUT_DESC_FIELD]] = Converter.convert(matches[desc[OUT_DESC_IDX]], desc[OUT_DESC_TYPE])
        res.append(obj)
    return res


@transformer
def format(input, **params):
    """
    Appends string formatted value to result
    :param input:
    :param params:
    :return:
    """
    PARAM_FORMAT_STRING = 'format.string'
    PARAM_FORMAT_INPUT = 'format.input'
    PARAM_RESULT_FIELD = 'result.field'
    IN_DESC_FIELD = 'field'
    IN_DESC_TYPE = 'type'

    format_string = params.get(PARAM_FORMAT_STRING)
    format_inputs = params.get(PARAM_FORMAT_INPUT)
    result_field = params.get(PARAM_RESULT_FIELD)
    for row in input:
        row_input = []
        for desc in format_inputs:
            row_input.append(Converter.convert(row[desc[IN_DESC_FIELD]], desc[IN_DESC_TYPE]))
        row[result_field] = format_string.format(*row_input)
    return input


@transformer
def left_join(input, **params):
    """
    Left join transformation
    :param input:
    :param params:
    :return:
    """
    PARAM_COL_RIGHT = 'col.right'
    PARAM_COL_LEFT = 'col.left'
    PARAM_FIELD_JOIN = 'field.join'

    right_df = pd.DataFrame.from_records(input[params.get(PARAM_COL_RIGHT)])
    left_df = pd.DataFrame.from_records(input[params.get(PARAM_COL_LEFT)])
    join_on = params.get(PARAM_FIELD_JOIN)
    res = right_df.set_index(join_on, drop=False).join(left_df.set_index(join_on, drop=False), on=[join_on], rsuffix='_right')
    return Converter.df2list(res)


@transformer
def union(input, **params):
    """
    Union transformation
    :param input:
    :param params:
    :return:
    """
    res = []
    for col in input:
        res.extend(input[col])
    return res


@transformer
def update_doc(input, **params):
    """
    Updates document with value from another document
    :param input:
    :param params:
    :return:
    """
    PARAM_SOURCE = 'source'
    SOURCE_COL = 'src.col'
    SOURCE_FIELD = 'src.field'
    PARAM_RESULT = 'result'

    res = input[params.get(PARAM_RESULT)]
    for src in params.get(PARAM_SOURCE):
        res[src[SOURCE_FIELD]] = input[src[SOURCE_COL]][src[SOURCE_FIELD]]
    return res


@transformer
def update_col(input, **params):
    """
    Updates document with value from another document/collection/constant
    :param input:
    :param params:
    :return:
    """
    PARAM_TARGET = 'target'
    PARAM_UPDATE = 'update'
    SOURCE_TYPE = 'src.type'
    SOURCE_COL = 'src.col'
    SOURCE_FIELD = 'src.field'
    DEST_FIELD = 'dest.field'
    SOURCE_TYPE_DOC = 'doc'
    SOURCE_TYPE_CONST = 'const'
    CONST_VALUE = 'const.value'

    update_list = params.get(PARAM_UPDATE)
    res = input[params.get(PARAM_TARGET)] if PARAM_TARGET in params else input
    for row in res:
        for update_desc in update_list:
            if update_desc[SOURCE_TYPE] == SOURCE_TYPE_DOC:
                row[update_desc[DEST_FIELD]] = input[update_desc[SOURCE_COL]][update_desc[SOURCE_FIELD]]
            elif update_desc[SOURCE_TYPE] == SOURCE_TYPE_CONST:
                row[update_desc[DEST_FIELD]] = update_desc[CONST_VALUE]
    return res


@transformer
def replace(input, **params):
    """
    Replaces field value
    :param input:
    :param params:
    :return:
    """
    PARAM_REPLACE_LIST = 'replace'
    REPLACE_FIELD = 'field'
    REPLACE_FIND_VALUE = 'value.to_find'
    REPLACE_WITH_VALUE = 'value.replace_with'

    replace_list = params.get(PARAM_REPLACE_LIST)
    for row in input:
        for replace in replace_list:
            if row[replace[REPLACE_FIELD]] == replace[REPLACE_FIND_VALUE]:
                row[replace[REPLACE_FIELD]] = replace[REPLACE_WITH_VALUE]
    return input


@transformer
def rename_fields(input, **params):
    """
    Renames field in collection
    :param input:
    :param params:
    :return:
    """
    PARAM_RENAME_LIST = 'rename'
    RENAME_SRC_FIELD = 'src.field'
    RENAME_DEST_FIELD = 'dest.field'

    rename_list = params.get(PARAM_RENAME_LIST)
    for row in input:
        for rename in rename_list:
            row[rename[RENAME_DEST_FIELD]] = row[rename[RENAME_SRC_FIELD]]
            row.pop(rename[RENAME_SRC_FIELD])
    return input


@transformer
def list_concat(input, **params):
    """
    Concatenates two or more lists and put result into dest field
    :param input:
    :param params:
    :return:
    """
    PARAM_SOURCE_FIELDS = 'src.fields'
    PARAM_DEST_FIELD = 'dest.field'

    field_list = params.get(PARAM_SOURCE_FIELDS)
    for row in input:
        res = []
        for field in field_list:
            res += row[field]
        row[params.get(PARAM_DEST_FIELD)] = res
    return input
