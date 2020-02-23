from .queryBuilder import QueryBuilder
from pydruid.utils.aggregators import count, cardinality, longsum, doublesum
from utils.exceptions import ParseArgException


class GroupByBuilder(QueryBuilder):

    def __init__(self, kwargs):
        super().__init__(kwargs)
        self._query_type = "groupby"

    def build_query(self):
        query = {"datasource": self.datasource}

        metric = self._parse_metric()
        granularity = self._parse_granularity()
        interval = self._parse_interval()
        dimension = self._parse_dimension()

        filter = self._parse_filters() if self._filters else None

        query.update(metric)
        query.update(granularity)
        query.update(interval)
        query.update(dimension)

        if filter:
            query.update(filter)

        return query

    def _parse_metric(self):
        if self._metric == 'uv':
            return {"aggregations": {"result": cardinality(self._field)}}

        elif self._metric == 'pv':
            return {"aggregations": {"result": count(self._field)}}

        elif self._metric == 'longsum':
            return {"aggregations": {"result": longsum(self._field)}}

        elif self._metric == 'doublesum':
            return {"aggregations": {"result": doublesum(self._field)}}

        else:
            raise ParseArgException("Parse metric failed")

    def _parse_dimension(self):
        return {'dimensions': self._dimension}

    def format_result(self, results):
        if not len(results):
            return []

        return [{'timestamp': i['timestamp'], 'result': i['event']} for i in results]
