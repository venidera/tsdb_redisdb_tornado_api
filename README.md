# Venidera's RedisDB Time Series Database - vRedisTSDB

A microservice with the business capability to store and retrieve time series data points.

The data is stored on RedisDB using the module *redistimeseries*.

# Specifications

### What it does

* Store data points from time series for a *key*
* Return data points from a time series identified by a *key*
* Allow retrieve data by intervals defined by *start* and *end*
* Allow retrieve timestamps as date time strings
* Allow aggregation *(avg, sum, min, max, range, count, first, last)* considering a time granularity value in secondess *(bucketSizeSeconds - time bucket for aggregation in seconds)*. Defaults: *avg* for *day* (86400 s)

### What it doesn't

* Remove points, only entire timeseries (RedisDB: *del <key>*)
* Store metadata

### What it will do:

* Store negative timestamps by using an inverted structure with a key that will identify each part of the time series (before and after epoch).
* Store min and max timestamps for a tskey (time series key)

### RESTful API

* Endpoints (Resources): '/tsdb
* Verbs: POST (submit data), GET (retrieve data) and DELETE (remove time series)
* Schema: ...
* Query arguments:
    * start: ...
    * end: ...
    * tstype: ...
    * ...

### Implementations:

* Python (>= 3.7): initial version implemented

* Golang: using a template to be implemented.



## Moving data from OpenTSDB to the microservice

Example REST GET request on OpenTSDB:

```
http://192.168.1.10:4242/api/query?start=1970/01/01-00:00:00&end=2100/01/01-00:00:00&m=sum:ts81_0&key=0
```

Response example:

```
[
    {
        "metric": "ts81_0",
        "tags": {},
        "aggregateTags": [
            "key"
        ],
        "dps": {
            "1444878000": 37591,
            "1444964400": 37588.641277641276,
            "1446343200": 37551,
            ...
            "1548727200": 38586.882352941175,
            "1548900000": 38589
        }
    }
]
```


## References

https://oss.redislabs.com/redistimeseries
https://oss.redislabs.com/redistimeseries/commands/
https://pt.slideshare.net/RedisLabs/redisconf18-redis-as-a-timeseries-db?cf_lbyyhhwhyjj5l3rs65cb3w=ey0o1s0f9ovz3l8qdt31a
https://github.com/RedisLabsModules/RedisTimeSeries
