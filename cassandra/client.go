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
	createTableCQL    = "CREATE TABLE IF NOT EXISTS snap.metrics (ns  text, ver int, host text, time timestamp, valType text, doubleVal double, strVal text, boolVal boolean, tags map<text,text>, PRIMARY KEY ((ns, ver, host), time),) WITH CLUSTERING ORDER BY (time DESC);"
	createTagTableCQL = "CREATE TABLE IF NOT EXISTS snap.tags (key  text, val text, time timestamp, ns text, ver int, host text, valType text, doubleVal double, strVal text, boolVal boolean, tags map<text,text>, PRIMARY KEY ((key, val), time),) WITH CLUSTERING ORDER BY (time DESC);"
)

// NewCassaClient creates a new instance of a cassandra client.
func NewCassaClient(co clientOptions, tagIndex string) *cassaClient {
	return &cassaClient{session: getInstance(co), tagsIndex: tagIndex}
}

// cassaClient contains a long running Cassandra CQL session
type cassaClient struct {
	session   *gocql.Session
	tagsIndex string
}

type clientOptions struct {
	server string
	ssl    *sslOptions
}

// sslOptions contains configuration for encrypted communication between the app and the server
type sslOptions struct {
	username                     string
	password                     string
	keyPath                      string
	certPath                     string
	caPath                       string
	enableServerCertVerification bool
}

var instance *gocql.Session
var once sync.Once

// getInstance returns the singleton of *gocql.Session. It is configured with ssl options if any are given.
// the session is not closed if the publisher is running.
func getInstance(co clientOptions) *gocql.Session {
	once.Do(func() {
		instance = getSession(co)
	})
	return instance
}

func (cc *cassaClient) saveMetrics(mts []plugin.MetricType) error {
	errs := []string{}
	var err error
	for _, m := range mts {
		// insert data into metrics table
		err = worker(cc.session, m)
		if err != nil {
			errs = append(errs, err.Error())
		}

		// inserts data into tags table if tagIndex config exists
		vtags := getValidTagIndex(m.Tags(), cc.tagsIndex)
		err = tagWorker(cc.session, m, vtags)
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		err = fmt.Errorf(strings.Join(errs, ";"))
	}
	return err
}

// works insert data into Cassandra DB metrics table only when the data is valid
func worker(s *gocql.Session, m plugin.MetricType) error {
	value, err := convert(m.Data())
	if err != nil {
		cassaLog.WithFields(log.Fields{
			"err": err,
		}).Error("Cassandra client invalid data type")
		return err
	}

	switch value.(type) {
	case float64:
		if err = s.Query(`INSERT INTO snap.metrics (ns, ver, host, time, valtype, doubleVal, tags) VALUES (?, ?, ?, ? ,?, ?, ?)`,
			m.Namespace().String(),
			m.Version(),
			m.Tags()[core.STD_TAG_PLUGIN_RUNNING_ON],
			time.Now(),
			"doubleval",
			value,
			m.Tags()).Exec(); err != nil {
			cassaLog.WithFields(log.Fields{
				"err": err,
			}).Error("Cassandra client insertion error")
			return err
		}
	case string:
		if err = s.Query(`INSERT INTO snap.metrics (ns, ver, host, time, valtype, strVal, tags) VALUES (?, ?, ?, ? ,?, ?, ?)`,
			m.Namespace().String(),
			m.Version(),
			m.Tags()[core.STD_TAG_PLUGIN_RUNNING_ON],
			time.Now(),
			"strval",
			value,
			m.Tags()).Exec(); err != nil {
			cassaLog.WithFields(log.Fields{
				"err": err,
			}).Error("Cassandra client insertion error")
			return err
		}
	case bool:
		if err = s.Query(`INSERT INTO snap.metrics (ns, ver, host, time, valtype, boolVal, tags) VALUES (?, ?, ?, ? ,?, ?, ?)`,
			m.Namespace().String(),
			m.Version(),
			m.Tags()[core.STD_TAG_PLUGIN_RUNNING_ON],
			time.Now(),
			"boolval",
			value,
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

// tagWorker insert data into Cassandra DB tags only when the tags array is not empty.
func tagWorker(s *gocql.Session, m plugin.MetricType, tags []string) error {
	if len(tags) == 0 {
		return nil
	}

	value, err := convert(m.Data())
	if err != nil {
		cassaLog.WithFields(log.Fields{
			"err": err,
		}).Error("Cassandra client invalid data type")
		return err
	}

	switch value.(type) {
	case float64:
		for _, v := range tags {
			if err = s.Query(`INSERT INTO snap.tags (key, val, time, ns, ver, host, valtype, doubleVal, tags) VALUES (?, ?, ?, ? ,?, ?, ?, ?, ?)`,
				v,
				m.Tags()[v],
				time.Now(),
				m.Namespace().String(),
				m.Version(),
				m.Tags()[core.STD_TAG_PLUGIN_RUNNING_ON],
				"doubleval",
				value,
				m.Tags()).Exec(); err != nil {
				cassaLog.WithFields(log.Fields{
					"err": err,
				}).Error("Cassandra client insertion error")
				return err
			}
		}
	case string:
		for _, v := range tags {
			if err = s.Query(`INSERT INTO snap.tags (key, val, time, ns, ver, host, valtype, strVal, tags) VALUES (?, ?, ?, ? ,?, ?, ?, ?, ?)`,
				v,
				m.Tags()[v],
				time.Now(),
				m.Namespace().String(),
				m.Version(),
				m.Tags()[core.STD_TAG_PLUGIN_RUNNING_ON],
				"strval",
				value,
				m.Tags()).Exec(); err != nil {
				cassaLog.WithFields(log.Fields{
					"err": err,
				}).Error("Cassandra client insertion error")
				return err
			}
		}
	case bool:
		for _, v := range tags {
			if err = s.Query(`INSERT INTO snap.tags (key, val, time, ns, ver, host, valtype, boolVal, tags) VALUES (?, ?, ?, ? ,?, ?, ?, ?, ?)`,
				v,
				m.Tags()[v],
				time.Now(),
				m.Namespace().String(),
				m.Version(),
				m.Tags()[core.STD_TAG_PLUGIN_RUNNING_ON],
				"boolval",
				value,
				m.Tags()).Exec(); err != nil {
				cassaLog.WithFields(log.Fields{
					"err": err,
				}).Error("Cassandra client insertion error")
				return err
			}
		}
	default:
		return fmt.Errorf(ErrInvalidDataType.Error(), value)
	}
	return nil
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

func createCluster(server string) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(server)
	cluster.Consistency = gocql.One
	cluster.ProtoVersion = 4
	return cluster
}

func getSession(co clientOptions) *gocql.Session {
	cluster := createCluster(co.server)

	if co.ssl != nil {
		cluster = addSslOptions(cluster, co.ssl)
	}

	session := initializeSession(cluster)
	return session
}

func addSslOptions(cluster *gocql.ClusterConfig, options *sslOptions) *gocql.ClusterConfig {
	// Add authentication if username and password were set.
	if options.username != "" && options.password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: options.username,
			Password: options.password}
	}

	sslOpts := &gocql.SslOptions{
		EnableHostVerification: options.enableServerCertVerification,
	}

	// All paths are optional depending on server config. Set them only if they are not empty.
	if options.certPath != "" {
		sslOpts.CertPath = options.certPath
	}
	if options.caPath != "" {
		sslOpts.CaPath = options.caPath
	}
	if options.keyPath != "" {
		sslOpts.KeyPath = options.keyPath
	}

	cluster.SslOpts = sslOpts
	return cluster
}

func initializeSession(cluster *gocql.ClusterConfig) *gocql.Session {
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

	if err := session.Query(createTagTableCQL).Exec(); err != nil {
		log.Fatal(err.Error())
	}
	return session
}

// getValidTagIndex checks if there are tags to be indexed for a giving metric.
func getValidTagIndex(mtag map[string]string, tagIndex string) []string {
	itags := []string{}

	indexTags := strings.Split(tagIndex, ",")
	for _, t := range indexTags {
		tt := strings.TrimSpace(t)
		if _, ok := mtag[tt]; ok {
			itags = append(itags, tt)
		}
	}
	return itags
}
