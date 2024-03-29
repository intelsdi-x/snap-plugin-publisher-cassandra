DISCONTINUATION OF PROJECT. 

This project will no longer be maintained by Intel.

This project has been identified as having known security escapes.

Intel has ceased development and contributions including, but not limited to, maintenance, bug fixes, new releases, or updates, to this project.  

Intel no longer accepts patches to this project.

# DISCONTINUATION OF PROJECT 

**This project will no longer be maintained by Intel.  Intel will not provide or guarantee development of or support for this project, including but not limited to, maintenance, bug fixes, new releases or updates.  Patches to this project are no longer accepted by Intel. If you have an ongoing need to use this project, are interested in independently developing it, or would like to maintain patches for the community, please create your own fork of the project.**


# Snap Cassandra publisher plugin 
This plugin publishes snap metric data into Cassandra database.

It's used in the [snap framework](http://github.com/intelsdi-x/snap).

1. [Getting Started](#getting-started)
  * [System Requirements](#system-requirements)
  * [Operating Systems](#operating-systems)
  * [Installation](#installation)
2. [Documentation](#documentation)
  * [Collected Metrics](#collected-metrics)
  * [Examples](#examples)
  * [Roadmap](#roadmap)
3. [Community Support](#community-support)
4. [Contributing](#contributing)
5. [License](#license-and-authors)
6. [Acknowledgements](#acknowledgements)

## Getting Started
### System Requirements
* [snap] (https://github.com/intelsdi-x/snap)
* [cassandra latest(3.3)](http://cassandra.apache.org)
* [golang 1.6+](https://golang.org/dl/)

### Operating systems
All OSs currently supported by snap:
* Linux/amd64
* Darwin/amd64

### Installation
#### Download cassandra publisher plugin binary:
You can get the pre-built binaries for your OS and architecture at snap's [Github Releases](https://github.com/intelsdi-x/snap/releases) page.

#### Building from source
* Get the package: 
```go get github.com/intelsdi-x/snap-plugin-publisher-cassandra```
* Build the snap-plugin-publisher-cassandra plugin
1. From the root of the snap-plugin-publisher-cassandra path type ```make all```.
* This builds the plugin in `/build/rootfs/`.

#### Configuration and Usage
* Set up the [snap framework](https://github.com/intelsdi-x/snap/blob/master/README.md#getting-started)
* Ensure `$SNAP_PATH` is exported
`export SNAP_PATH=$GOPATH/src/github.com/intelsdi-x/snap/build`
* Ensure 'server' is defined in the task manifest. 
* `$SNAP_CASSANDRA_HOST` may be exported only for the integration/unit testing

#### Install Cassandra
* install Cassandra using Docker
```
 docker run --name snap-cassandra -p 9042:9042 -d cassandra:latest
```
* install Cassandra on Mac OSX
```
 brew install cassandra
```

## Documentation

### Suitable Metrics
All metrics exposed by snap collector plugins. Currently, it only supports the number, string, and boolean
data types. Number data types are integers and floats. Plugin stores numbers inside Cassandra as doubles.

### Plugin Database Schema
Metric data always goes in the table _`metrics`_ of the keyspace _`snap`_. The primary key for table metrics is the combination of a metric namespace, version, and the running host.
```
CREATE TABLE snap.metrics (
	ns  text, 
	ver int, 
	host text, 
	time timestamp, 
    valtype text,
	doubleVal double, 
    boolVal boolean,
    strVal text,
	tags map<text,text>, 
	PRIMARY KEY ((ns, ver, host), time))
) WITH CLUSTERING ORDER BY (time DESC);
```

Metric data goes into the table _`tag`_ only when the _`tagIndex`_ is giving in a publisher config. _`tagIndex`_ is a comma separatored tag list. Please refer to [here](./docs/TABLES.md) for details.
```
CREATE TABLE snap.tags (
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

### Examples
Let's get started. For example:

![Dockerized example](https://media.giphy.com/media/3osxYsKQ02KRyq5QrK/giphy.gif)

Install Cassandra
``` 
$  docker run --name snap-cassandra-1 -p 9042:9042 -d cassandra:latest
17d007776bcd9efa55d89640dcfe9e3ff4baf54468eb3ab8716270d95adc262c
```
Verify Cassandra Instance
```
$ docker run -it --rm --net container:snap-cassandra-1 cassandra cqlsh
Connected to Test Cluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.3 | CQL spec 3.4.0 | Native protocol v4]
Use HELP for help.
cqlsh> desc keyspaces;

system_traces  system_schema  system_auth  system  system_distributed

cqlsh> 
```

In one terminal window, start the snap daemon (in this case with logging set to 1 and trust disabled):
```
$ $SNAP_PATH/bin/snapd -l 1 -t 0
INFO[0000] Starting snapd (version: v0.11.0-beta-84-g3b1ae75) 
INFO[0000] setting GOMAXPROCS to: 1 core(s)             
INFO[0000] control started                               _block=start _module=control
INFO[0000] module started                                _module=snapd block=main snap-module=control
```

In another terminal window:
Load snap collector plugins. For example:
```
$ $SNAP_PATH/bin/snapctl plugin load $SNAP_PATH/../../snap-plugin-collector-psutil/build/rootfs/snap-plugin-collector-psutil
Plugin loaded
Name: psutil
Version: 5
Type: collector
Signed: false
Loaded Time: Thu, 25 Feb 2016 13:00:24 PST
```

Load snap passthrough processor plugin
```
$ $SNAP_PATH/bin/snapctl plugin load $SNAP_PATH/plugin/snap-plugin-processor-passthru
Plugin loaded
Name: passthru
Version: 1
Type: processor
Signed: false
Loaded Time: Thu, 25 Feb 2016 13:00:35 PST
```
Load snap cassandra publisher plugin
```
$ $SNAP_PATH/bin/snapctl plugin load <path to snap-plugin-publisher-cassandra plugin binary>
Plugin loaded
Name: cassandra
Version: 1
Type: publisher
Signed: false
Loaded Time: Thu, 25 Feb 2016 13:00:48 PST

```
Create tasks. For example:
```
$ $SNAP_PATH/bin/snapctl task create -t /tmp/cassandra-task.json
Using task manifest to create task
Task created
ID: f5dda751-c4db-4361-8a63-ced153aa6550
Name: Task-f5dda751-c4db-4361-8a63-ced153aa6550
State: Running

```
The example task manifest file, cassandra-task.json. Note that _`server`_ in the publisher config is mandatory but _`tagIndex`_ is optional.
Specifying _`tagIndex`_ only when you like to do _read queries_ for tags.
```json
{
    "_comment": "tagIndex in the publish config is optional",
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
                        "server": "SNAP_CASSANDRA_HOST",
                        "tagIndex": "experimentId,scope"
                    }
                }
            ]                                            
        }
    }
}
```

Snap cassandra publisher allows also for configuring ssl encryption.
To enable ssl, a flag `ssl` has to be set to true in the plugin config.
It is possible to configure following ssl options:
* `username` - Name of a user used to authenticate to Cassandra
* `password` - Password used to authenticate to the Cassandra
* `keyPath` - Path to the private key for the Cassandra client
* `certPath` - Path to the self signed certificate for the Cassandra client
* `caPath` - Path to the CA certificate for the Cassandra server
* `serverCertVerification` - If true, verify a hostname and a server key, default: true

Sample snap cassandra CQL shown:
```
cqlsh:snap> select * from metrics limit 100;

 ns                      | ver | host          | time                     | boolval | doubleval | strval | tags | valtype
-------------------------+-----+---------------+--------------------------+---------+-----------+--------+------+---------
 intel/psutil/load/load1 |   0 | egu-mac01.lan | 2016-03-29 03:04:52+0000 |    null |      2.44 |   null | null |  double
 intel/psutil/load/load1 |   0 | egu-mac01.lan | 2016-03-29 03:04:51+0000 |    null |      2.44 |   null | null |  double
 intel/psutil/load/load1 |   0 | egu-mac01.lan | 2016-03-29 03:04:50+0000 |    null |      2.57 |   null | null |  double
 intel/psutil/load/load1 |   0 | egu-mac01.lan | 2016-03-29 03:04:49+0000 |    null |      2.57 |   null | null |  double
 intel/psutil/load/load1 |   0 | egu-mac01.lan | 2016-03-29 03:04:48+0000 |    null |      2.57 |   null | null |  double
 ...
 intel/psutil/load/load1 |   0 | egu-mac01.lan | 2016-03-29 03:03:15+0000 |    null |      3.22 |   null | null |  double
 intel/psutil/load/load1 |   0 | egu-mac01.lan | 2016-03-29 03:03:14+0000 |    null |      3.22 |   null | null |  double
 intel/psutil/load/load1 |   0 | egu-mac01.lan | 2016-03-29 03:03:13+0000 |    null |      3.22 |   null | null |  double

(100 rows)
cqlsh:snap>
```

### Roadmap
This plugin is still in active development. As we launch this plugin, we have a few items in mind for the next few releases: 
 * Additional error handling
 * Testing in a large cluster
 
If you have a feature request, please add it as an [issue](https://github.com/intelsdi-x/snap-plugin-publisher-cassandra/issues/new) and/or 
submit a [pull request](https://github.com/intelsdi-x/snap-plugin-publisher-cassandra/pulls).

## Community Support
This repository is one of **many** plugins in **snap**, a powerful telemetry framework. See the full project at http://github.com/intelsdi-x/snap.

To reach out to other users, head to the [main framework](https://github.com/intelsdi-x/snap#community-support).

## Contributing
We love contributions!

There's more than one way to give back, from examples to blogs to code updates. See our recommended process in [CONTRIBUTING.md](CONTRIBUTING.md).

## License
[snap](http://github.com:intelsdi-x/snap), along with this plugin, is an Open Source software released under the Apache 2.0 [License](LICENSE).

## Acknowledgements
* Author: [@candysmurf](https://github.com/candysmurf)
* Author: [@jcooklin](https://github.com/jcooklin)

And **thank you!** Your contribution, through code and participation, is incredibly important to us.

