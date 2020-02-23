from pydruid.async_client import AsyncPyDruid
from pydruid.client import PyDruid

class DruidClient:

    def __init__(self, address, port=8082):
        url = f"http://{address}:{port}"
        self.async_client = AsyncPyDruid(url, 'druid/v2/')
        self.client = PyDruid(url, 'druid/v2/')

    async def async_run(self, queryBuilder):
        t_query = queryBuilder._query_type
        query = queryBuilder.build_query()

        method = getattr(self.async_client, t_query)
        results = await method(**query)
        return queryBuilder.format_result(results)


    def run(self, queryBuilder):
        t_query = queryBuilder._query_type
        query = queryBuilder.build_query()
        method = getattr(self.client, t_query)
        results = method(**query)
        return queryBuilder.format_result(results)
