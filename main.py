from druid.client import DruidClient
from druid.timeseries import TimeSeriesBuilder



def main():
    param = {
        "datasource": "xxxx",
        "metric": "pv",
        "field": "xxx",
        "granularity": "pt1m",
        "interval": "2020-02-22T21:32:00+08:00/2020-02-22T21:33:00+08:00"
    }

    ts = TimeSeriesBuilder(param)
    c = DruidClient("127.0.0.1", 8888)
    res =  c.run(ts)
    print(res)



if __name__ == '__main__':
    main()