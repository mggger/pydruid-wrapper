from pydruid.utils.aggregators import count, cardinality, thetasketch, longsum
from utils.exceptions import ParseArgException
from .queryBuilder import QueryBuilder


class TopNBuilder(QueryBuilder):

    def __init__(self, kwargs):
        super().__init__(kwargs)
        self._query_type = "topn"

    def build_query(self):
        query = {"datasource": self.datasource}

        metric = self._parse_metric()
        granularity = self._parse_granularity()
        interval = self._parse_interval()
        dimension = self._parse_dimension()

        filter = self._parse_filters() if self._filters else None
        limit = self._parse_limit() if self._limit else None

        query.update(metric)
        query.update(granularity)
        query.update(interval)
        query.update(dimension)

        if filter:
            query.update(filter)

        if limit:
            query.update(limit)

        return query

    def _parse_metric(self):
        if self._metric == 'uv':
            return {"aggregations": {"result": cardinality(self._field)},
                    "metric": "result"
                    }

        elif self._metric == 'exact_uv':
            return {"aggregations": {"result": thetasketch(self._field)},
                    "metric": "result"
                    }

        elif self._metric == 'pv':
            return {"aggregations": {"result": count(self._field)},
                    "metric": "result"}

        elif self._metric == 'longsum':
            return {"aggregations": {"result": longsum(self._field)},
                    "metric": "result"}


        else:
            raise ParseArgException("Parse metric failed")

    def _parse_dimension(self):
        return {'dimension': self._dimension}

    def format_result(self, results):

        if not len(results):
            return []

        result = results.result[0]['result']

        maxDepth = min(len(result), self._limit)


        return [{self._dimension: i[self._dimension], 'result': i['result']}
                    for i in result[:maxDepth]]

