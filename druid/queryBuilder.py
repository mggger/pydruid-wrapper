from pydruid.utils.filters import Filter
from utils.exceptions import ParseArgException



class QueryBuilder():
    def __init__(self, kwargs):
        self.datasource = kwargs.get("datasource", None)
        self._metric = kwargs.get("metric", None)
        self._field = kwargs.get("field", None)
        self._granularity = kwargs.get("granularity", None)
        self._interval = kwargs.get("interval", None)
        self._dimension = kwargs.get("dimension", None)
        self._filters = kwargs.get("filter", None)
        self._limit = kwargs.get("limit", None)
        self._query_type = None

    def build_query(self):
        raise NotImplemented

    def _parse_metric(self):
        raise NotImplemented

    def _parse_granularity(self):
        granularity_dict = {
            "pt1m": "minute",
            "pt1h": "hour",
            "p1d": "day",
            "p1w": "week",
            "p1m": "month"
        }

        if self._granularity in granularity_dict.keys():
            return {"granularity": {"type": "period", "period": self._granularity, "timeZone": "Asia/Shanghai"}}

        elif self._granularity == "all":
            return {"granularity": "all"}

        else:
            raise ParseArgException("Parse granularity failed")

    def _parse_interval(self):
        return {"intervals": self._interval}

    def _parse_dimension(self):
        pass

    def _parse_filters(self):
        try:
            _type = self._filters['type']
            filters = []

            for f in self._filters['filters']:
                _filter = self._parse_filter(f)
                filters.append(_filter)

            if _type == 'and':
                return {"filter": Filter(type='and', fields=filters)}

            elif _type == 'or':
                return {"filter": Filter(type='or', fields=filters)}

            elif _type == 'none':
                return {"filter": filters[0]}

            else:
                raise ParseArgException("Parse filter failed")

        except:
            raise ParseArgException("Parse filter failed")

    def _parse_filter(self, filter):
        _type = filter['type']
        col = filter['col']
        op = filter['op']
        val = filter['val']
        isNumberCol = filter['number_col'] == "true"

        if _type == 'bound':

            op_dict = {
                ">": Filter(type="bound", dimension=col, lowerStrict=True, upperStrict=False, lower=val, upper=None,
                            alphaNumeric=isNumberCol),

                ">=": Filter(type='bound', dimension=col, lowerStrict=False, upperStrict=False, lower=val, upper=None,
                             alphaNumeric=isNumberCol),

                "<": Filter(type='bound', dimension=col, lowerStrict=False, upperStrict=False, lower=None, upper=val,
                            alphaNumeric=isNumberCol),

                "<=": Filter(type='bound', dimension=col, lowerStrict=False, upperStrict=False, lower=None, upper=val,
                             alphaNumeric=isNumberCol),

                "==": Filter(dimension=col, value=val),

                '!=': ~Filter(dimension=col, value=val)
            }

            cond = op_dict[op]


        elif _type == 'in':
            cond = Filter(dimension=col, values=val, type='in')

        elif _type == 'not in':
            cond = ~Filter(dimension=col, values=val, type='in')

        elif _type == 'like':
            cond = Filter(dimension=col, pattern=val, type='regex')

        elif _type == 'interval':
            cond = Filter(dimension=col, intervals=val, type='interval')

        else:
            raise ParseArgException("Parse filter failed")

        return cond

    def _parse_limit(self):
        return ({"threshold": self._limit})

    def format_result(self):
        raise NotImplemented
