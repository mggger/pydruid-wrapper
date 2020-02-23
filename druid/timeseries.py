from .queryBuilder import QueryBuilder
from utils.exceptions import ParseArgException
from pydruid.utils.aggregators import count, cardinality, longsum, doublesum, thetasketch


class TimeSeriesBuilder(QueryBuilder):

    def __init__(self, kwargs):

        super().__init__(kwargs)
        self._query_type = "timeseries"

    def build_query(self):
        query = {"datasource": self.datasource}

        metric = self._parse_metric()
        granularity = self._parse_granularity()
        interval = self._parse_interval()

        filter = self._parse_filters() if self._filters else None
        limit = self._parse_limit() if self._limit else None

        query.update(metric)
        query.update(granularity)
        query.update(interval)

        if filter:
            query.update(filter)

        if limit:
            query.update(limit)

        return query

    def _parse_metric(self):
        if self._metric == 'uv':
            return {"aggregations": {"result": cardinality(self._field)}}

        elif self._metric == 'exact_uv':
            return {"aggregations": {"result": thetasketch(self._field)}}

        elif self._metric == 'pv':
            return {"aggregations": {"result": count(self._field)}}

        elif self._metric == 'longsum':
            return {"aggregations": {"result": longsum(self._field)}}

        elif self._metric == 'doublesum':
            return {"aggregations": {"result": doublesum(self._field)}}

        else:
            raise ParseArgException("Parse metric failed")

    def format_result(self, result):
        if not len(result):
            return []

        if self._metric == 'uv' or self._metric == 'exact_uv':
            return [{'timestamp': i['timestamp'], 'result': int(i['result']['result'])} for i in result]

        else:
            return [{'timestamp': i['timestamp'], 'result': i['result']['result']} for i in result]

