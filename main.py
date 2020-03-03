from druid.client import DruidClient
from druid.timeseries import TimeSeriesBuilder
from druid.topn import TopNBuilder
from pydruid.utils.aggregators import count, cardinality, thetasketch, longsum
from pydruid.utils.filters import Dimension

def main():
    param = {
        'datasource': 'mudu_prod_events',
         'metric': 'longsum',
         'interval': '2020-03-03T10:10:00.000+0800/2020-03-03T10:50:00.000+0800',
         'field': 'event_val',
         'dimension': 'page',
         'granularity': 'all',
         'filter':
             {'type': 'none',
              'filters': [
                {'type': 'bound',
                 'col': 'category',
                 'number_col': 'false',
                 'op': '==',
                 'val': 'live_watch_time'
                 }
              ]
            },
        'limit': 10
    }

    ts = TopNBuilder(param)
    c = DruidClient("127.0.0.1", 8888)

    res = c.run(ts)
    print(res)


if __name__ == '__main__':
    main()
