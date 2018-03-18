from datetime import date
from flask.json import JSONEncoder
from jsondiff import diff
from na3x.utils.converter import Converter, Types


class ExtJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, date):
                if obj.hour != 0: # ToDo: fix this work around
                    return Converter.datetime2str(obj)
                else:
                    return Converter.convert(obj, Types.TYPE_STRING)
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)


class JSONUtils():
    DIFF_DELETE = '$delete'

    def diff(a, b):
        delta = diff(a, b)
        if not a:
            return delta
        for item in delta:
            if isinstance(a[item], list):
                if delta[item] == []:
                    delta[item] = {JSONUtils.DIFF_DELETE : a[item]}
                elif isinstance(delta[item], dict):
                    for key in delta[item].keys():
                        if str(key) == JSONUtils.DIFF_DELETE:
                            delta_upd = {JSONUtils.DIFF_DELETE : []}
                            for i in delta[item][key]:
                                delta_upd[JSONUtils.DIFF_DELETE].append(a[item][i])
                                delta[item] = delta_upd
        return delta
