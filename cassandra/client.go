/*
http://www.apache.org/licenses/LICENSE-2.0.txt


Copyright 2016 Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cassandra

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/core"
)

var (
	cassaLog           = log.WithField("_module", "snap-cassandra-clinet")
	ErrInvalidDataType = errors.New("Invalid data type value found - %v")

	createKeyspaceCQL = "CREATE KEYSPACE IF NOT EXISTS snap WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
	createTableCQL    = "CREATE TABLE IF NOT EXISTS snap.metrics (ns  text, ver int, host text, time timestamp, valType text, doubleVal double, strVal text, boolVal boolean, labels list<text>, tags map<text,text>, PRIMARY KEY ((ns, ver, host), time),) WITH CLUSTERING ORDER BY (time DESC);"
)

func NewCassaClient(server string) *cassaClient {
	return &cassaClient{session: getInstance(server)}
}

// cassaClient contains a long running Cassandra CQL session
type cassaClient struct {
	session *gocql.Session
}

var instance *gocql.Session
var once sync.Once

// getInstance returns the singleton of *gocql.Session.
// the sessio is not closed if the publisher is running.
func getInstance(server string) *gocql.Session {
	once.Do(func() {
		instance = getSession(server)
	})
	return instance
}

func (cc *cassaClient) saveMetrics(mts []plugin.PluginMetricType) error {
	errs := []string{}
	var err error
	for _, m := range mts {
		err = worker(cc.session, m)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		err = fmt.Errorf(strings.Join(errs, ";"))
	}
	return err
}

// works insert data into Cassandra DB only when the data is valid
func worker(s *gocql.Session, m plugin.PluginMetricType) error {
	value, err := convert(m.Data())
	if err != nil {
		cassaLog.WithFields(log.Fields{
			"err": err,
		}).Error("Cassandra client invalid data type")
		return err
	}

	labels := buildLabels(m.Labels())

	switch value.(type) {
	case float64:
		if err = s.Query(`INSERT INTO snap.metrics (ns, ver, host, time, valtype, doubleVal, labels, tags) VALUES (?, ?, ?, ? ,?, ?, ?, ?)`,
			strings.Join(m.Namespace(), "/"),
			m.Version(),
			m.Source(),
			time.Now(),
			"doubleval",
			value,
			labels,
			m.Tags()).Exec(); err != nil {
			cassaLog.WithFields(log.Fields{
				"err": err,
			}).Error("Cassandra client insertion error")
			return err
		}
	case string:
		if err = s.Query(`INSERT INTO snap.metrics (ns, ver, host, time, valtype, strVal, labels, tags) VALUES (?, ?, ?, ? ,?, ?, ?, ?)`,
			strings.Join(m.Namespace(), "/"),
			m.Version(),
			m.Source(),
			time.Now(),
			"strval",
			value,
			labels,
			m.Tags()).Exec(); err != nil {
			cassaLog.WithFields(log.Fields{
				"err": err,
			}).Error("Cassandra client insertion error")
			return err
		}
	case bool:
		if err = s.Query(`INSERT INTO snap.metrics (ns, ver, host, time, valtype, boolVal, labels, tags) VALUES (?, ?, ?, ? ,?, ?, ?, ?)`,
			strings.Join(m.Namespace(), "/"),
			m.Version(),
			m.Source(),
			time.Now(),
			"boolval",
			value,
			labels,
			m.Tags()).Exec(); err != nil {
			cassaLog.WithFields(log.Fields{
				"err": err,
			}).Error("Cassandra client insertion error")
			return err
		}
	default:
		return fmt.Errorf(ErrInvalidDataType.Error(), value)
	}
	return nil
}

func buildLabels(labels []core.Label) []string {
	results := []string{}
	if labels != nil {
		for _, la := range labels {
			out, err := json.Marshal(la)
			// logs the error only
			if err != nil {
				cassaLog.WithFields(log.Fields{
					"err": err,
				}).Error("Cassandra client build labels error")
			} else {
				results = append(results, string(out))
			}
		}
	}
	return results
}

// converts the value into float64 and filters out the
// invalid data
func convert(i interface{}) (interface{}, error) {
	var num interface{}
	var err error

	switch v := i.(type) {
	case float64:
		num = v
	case float32:
		num = float64(v)
	case int16:
		num = float64(v)
	case int32:
		num = float64(v)
	case int64:
		num = float64(v)
	case int8:
		num = float64(v)
	case uint64:
		num = float64(v)
	case uint32:
		num = float64(v)
	case uint16:
		num = float64(v)
	case uint8:
		num = float64(v)
	case uint:
		num = float64(v)
	case int:
		num = float64(v)
	case bool:
		num = v
	case string:
		num = v
	default:
		err = fmt.Errorf(ErrInvalidDataType.Error(), v)
	}
	return num, err
}

func getSession(server string) *gocql.Session {
	cluster := gocql.NewCluster(server)
	cluster.Consistency = gocql.One
	cluster.ProtoVersion = 4

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err.Error())
	}

	if err := session.Query(createKeyspaceCQL).Exec(); err != nil {
		log.Fatal(err.Error())
	}

	if err := session.Query(createTableCQL).Exec(); err != nil {
		log.Fatal(err.Error())
	}
	return session
}
