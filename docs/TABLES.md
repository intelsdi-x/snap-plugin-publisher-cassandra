
# Cassandra Tables and Queries

snap-plugin-publisher-cassandra creates keyspace _`snap`_ and tables _`metrics`_ and _`tags`_ to store Snap metric data. The table _`tag`_ is created if Snap task manifest specified a config item.

### Table metrics
Table _`metrics`_ stores Snap metric data with the metric namespace, version, host as the partition key and time as the cluster key. The data is ordered by the latest first. 

#### Table metrics design
Table _`metrics`_ is designed for querying metric data by a metric namespace, version and host.
```
CREATE TABLE IF NOT EXISTS snap.metrics (
    ns  text, 
    ver int, 
    host text, 
    time timestamp, 
    valType text, 
    doubleVal double, 
    strVal text, 
    boolVal boolean, 
    tags map<text,text>, 
    PRIMARY KEY ((ns, ver, host), time))
) WITH CLUSTERING ORDER BY (time DESC);
```

#### Query table metrics
For querying table _`metrics`_, its partition key(ns, ver, host) is mandatory. Cluster key is optional.

**Sample Queries**
```
SELECT * FROM METRICS
WHERE NS   = '/qux' 
  AND VER  =  0 
  AND HOST = 'hostname'

SELECT * FROM METRICS
WHERE NS   = '/foo' 
  AND VER  =  0 
  AND HOST = 'hostname' 
  AND TIME > '2016-07-02 22:11:01+0000'
  AND TIME < '2016-08-02 22:11:01+0000';
```

### Table tags
Table _`tags`_ stores Snap metric data with the metric tag key and value as the partition key and columns time as the cluster key. The data is ordered by the latest first. 

#### Table tags design
Table _`tags`_ is designed for querying metric data by a metric tag key, value and time.
 ```
CREATE TABLE IF NOT EXISTS snap.tags (
    key text,  
    val text,  
    time timestamp, 
    ns  text, 
    ver int,   
    host text,  
    valType text,   
    doubleVal double,   
    strVal text,   
    boolVal boolean,   
    tags map<text,text>,   
    PRIMARY KEY ((key, val), time, ns, ver, host))
) WITH CLUSTERING ORDER BY (time DESC);
 ```

#### Query table tags
For querying table _`tags`_, its partition key (key, val) is mandatory. Cluster key is optional.

**Sample Queries**
```
SELECT * FROM TAGS 
WHERE KEY = 'experimentId' 
  AND VAL = 'x';
  
SELECT * from TAGS 
WHERE KEY = 'experimentId' 
  AND VAL = 'x' 
  AND TIME >'2016-08-02 22:50:04+0000';
``` 
### Snap Task Manifest NoSQL specific
The table _`snap.tags`_ is created if the parameter _`tagIndex`_ is specified in the Snap publisher task manifest. Specifying this tag only when your use cases need to query on tags.
* `tagIndex`: A comma separated tag key list. e.g. experimentId,scope.

**Sample Task Manifest**
```
{
    "version": 1,
    "schedule": {
        "type": "simple",
        "interval": "1s"
    },
    "workflow": {
        "collect": {
            "metrics": {
                "/intel/psutil/load/load1": {},
                "/intel/psutil/load/load5": {},
                "/intel/psutil/load/load15": {},
                "/intel/psutil/vm/available": {},
                "/intel/psutil/vm/free": {},
                "/intel/psutil/vm/used": {}
            },
            "config": {},
            "process": null,
            "publish": [
                {
                    "plugin_name": "cassandra",                            
                    "config": {
                        "server": "SNAP_CASSANDRA_HOST"
                        "tagIndex": "experimentId,scope"
                    }
                }
            ]                                            
        }
    }
}
```
Note that table _`snap.tags`_ stores metric data tagged with keys defined in `tagIndex` config field.

If the current design does not fit your use cases, please add it as an [issue](https://github.com/intelsdi-x/snap-plugin-publisher-cassandra/issues/new) and/or 
submit a [pull request](https://github.com/intelsdi-x/snap-plugin-publisher-cassandra/pulls).
