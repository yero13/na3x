import pandas as pd


class Aggregator:
    """
    Aggregator
    """
    @staticmethod
    def agg_multi_func(values, agg_field, agg_funcs, group_by=None):
        """
        Aggregates several functions
        :param values: list of objects (dict)
        :param agg_field: target field to calculate aggregates
        :param agg_funcs: list of aggregate functions
        :param group_by: field used to determine group
        :return: dict {agg_func0: value, agg_func1: agg_value, ...}
        """
        if len(values) == 0:
            return None
        else:
            if group_by:
                group_aggs = pd.DataFrame(values).groupby(group_by).agg({agg_field: agg_funcs})
                res = {}
                for row in group_aggs.itertuples():
                    row_res = {}
                    for i, func in enumerate(agg_funcs):
                        row_res.update({func: row[i+1]})
                    res.update({row[0]: row_res})
                return res
            else:
                aggs = pd.DataFrame(values).agg({agg_field: agg_funcs})
                res = {}
                for func in agg_funcs:
                    res.update({func: aggs[agg_field][func]})
                return res

    @staticmethod
    def agg_single_func(values, agg_field, agg_func, group_by=None):
        """
        Aggregates single function
        :param values: list of objects (dict)
        :param agg_field: target field to calculate aggregate
        :param agg_func: aggregate function
        :param group_by: field used to determine group
        :return: aggregated value
        """
        if len(values) == 0:
            return None
        else:
            if group_by:
                group_aggs = pd.DataFrame(values).groupby(group_by).agg({agg_field: [agg_func]})
                res = {}
                for row in group_aggs.itertuples():
                    res.update({row[0]: row[1]})
                return res
            else:
                return pd.DataFrame(values).agg({agg_field: [agg_func]})[agg_field][agg_func]

