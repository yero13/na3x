import datetime
import logging


class Types:
    TYPE_FLOAT = 'float'
    TYPE_INT = 'int'
    TYPE_DATE = 'date'
    TYPE_DATETIME = 'datetime'
    TYPE_STRING = 'string'
    TYPE_ARRAY = 'array'
    TYPE_OBJECT = 'object'


class Converter:
    @staticmethod
    def convert(input, type):
        """
        Converts input value to request type
        :param input: input value
        :param type: type to cast
        :return: converted value
        """
        try:
            if not input:
                if type == Types.TYPE_STRING:
                    return ''
                else:
                    return None
            if type == Types.TYPE_STRING:
                if isinstance(input, datetime.date):
                    return input.strftime('%Y-%m-%d')
                else:
                    return input
            if type == Types.TYPE_FLOAT:
                return float(input)
            elif type == Types.TYPE_INT:
                return int(input)
            elif type == Types.TYPE_DATE:
                if isinstance(input, datetime.date):
                    return input
                else:
                    return datetime.datetime.strptime(input, '%Y-%m-%d')
            elif type == Types.TYPE_DATETIME:
                if isinstance(input, datetime.datetime):
                    return input
                else:
                    return datetime.datetime.strptime(input[0:10], '%Y-%m-%d') # 2017-09-18T18:53:00.000Z
            else:
                return NotImplementedError('Not supported type - {}'.format(type))
        except Exception as e:
            logging.error(e, exc_info=True)
            raise Exception(e)

    @staticmethod
    def datetime2str(input):
        """
        Converts datetime into string
        :param input: datetime
        :return: string
        """
        return input.strftime('%Y-%m-%d %H:%M')

    @staticmethod
    def df2list(df):
        """
        Converts pandas.DataFrame values into list
        :param df: pandas.DataFrame
        :return: list
        """
        for column in df:
            df[column] = df[column].astype(object).where(df[column].notnull(), None)
        return df.to_dict(orient='records')
