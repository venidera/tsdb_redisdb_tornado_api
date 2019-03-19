

## Create Docker with RedisDB Timeseries Module

```
$ docker run —-name redis_tsdb -p 192.168.1.100:6385:6379 -v $(pwd)/redisdb_tsdb_volume -d redislabs/redistimeseries
```

