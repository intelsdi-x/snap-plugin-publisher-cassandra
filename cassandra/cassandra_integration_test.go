// +build medium

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
	"bytes"
	"encoding/gob"
	"log"
	"os"
	"testing"
	"time"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/core"
	"github.com/intelsdi-x/snap/core/ctypes"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	connectionTimeout            = 2
	shouldCreateKeyspace         = true
	enableServerCertVerification = false
	ignorePeerAddr               = false
	initialHostLookup            = true
	keyspaceName                 = "snap"
	tableName                    = "foo"
	password                     = "password"
	port                         = 9042
	serverAddress                = "127.0.0.1"
	sslOptionsFlag               = true
	timeout                      = 2
	username                     = "username"
)

func TestCassandraPublish(t *testing.T) {
	config := make(map[string]ctypes.ConfigValue)
	ip := NewCassandraPublisher()

	Convey("snap plugin CassandraDB integration testing with Cassandra", t, func() {
		var buf bytes.Buffer

		hostip := os.Getenv("SNAP_CASSANDRA_HOST")
		if len(hostip) == 0 {
			log.Fatal("SNAP_CASSANDRA_HOST is not set")
		}

		config[connectionTimeoutRuleKey] = ctypes.ConfigValueInt{Value: connectionTimeout}
		config[createKeyspaceRuleKey] = ctypes.ConfigValueBool{Value: shouldCreateKeyspace}
		config[ignorePeerAddrRuleKey] = ctypes.ConfigValueBool{Value: ignorePeerAddr}
		config[initialHostLookupRuleKey] = ctypes.ConfigValueBool{Value: initialHostLookup}
		config[keyspaceNameRuleKey] = ctypes.ConfigValueStr{Value: keyspaceName}
		config[portRuleKey] = ctypes.ConfigValueInt{Value: port}
		config[serverAddrRuleKey] = ctypes.ConfigValueStr{Value: hostip}
		config[sslOptionsRuleKey] = ctypes.ConfigValueBool{Value: false}
		config[tagIndexRuleKey] = ctypes.ConfigValueStr{Value: "experimentId,mode,year"}
		config[timeoutRuleKey] = ctypes.ConfigValueInt{Value: timeout}
		config[tableNameRuleKey] = ctypes.ConfigValueStr{Value: tableName}

		Convey("Publish integer metric", func() {
			tags := map[string]string{core.STD_TAG_PLUGIN_RUNNING_ON: "hostname", "experimentId": "101"}
			metrics := []plugin.MetricType{
				*plugin.NewMetricType(core.NewNamespace("intel", "psutil", "load", "load1"), time.Now(), tags, "int", 103),
			}
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)
			err := ip.Publish(plugin.SnapGOBContentType, buf.Bytes(), config)
			So(err, ShouldBeNil)
		})

		Convey("Publish float metric", func() {
			tags := map[string]string{core.STD_TAG_PLUGIN_RUNNING_ON: "hostname", "experimentId": "102"}
			metrics := []plugin.MetricType{
				*plugin.NewMetricType(core.NewNamespace("intel", "psutil", "load", "load5"), time.Now(), tags, "float64", 3.141),
			}
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)
			err := ip.Publish(plugin.SnapGOBContentType, buf.Bytes(), config)
			So(err, ShouldBeNil)
		})

		Convey("Publish string metric", func() {
			tags := map[string]string{core.STD_TAG_PLUGIN_RUNNING_ON: "hostname", "experimentId": "103", "mode": "abc"}
			metrics := []plugin.MetricType{
				*plugin.NewMetricType(core.NewNamespace("intel", "psutil", "load", "load15"), time.Now(), tags, "string", "bar"),
			}
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)
			err := ip.Publish(plugin.SnapGOBContentType, buf.Bytes(), config)
			So(err, ShouldBeNil)
		})

		Convey("Publish boolean metric", func() {
			tags := map[string]string{core.STD_TAG_PLUGIN_RUNNING_ON: "hostname", "experimentId": "104", "brand": "xyz"}
			metrics := []plugin.MetricType{
				*plugin.NewMetricType(core.NewNamespace("baz"), time.Now(), tags, "bool", true),
			}
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)
			err := ip.Publish(plugin.SnapGOBContentType, buf.Bytes(), config)
			So(err, ShouldBeNil)
		})

		Convey("Publish map metric", func() {
			tags := map[string]string{core.STD_TAG_PLUGIN_RUNNING_ON: "hostname", "experimentId": "105", "year": "2016"}
			metrics := []plugin.MetricType{
				*plugin.NewMetricType(core.NewNamespace("invalid/data/type"), time.Now(), tags, "map", map[string]string{"foo": "bar"}),
			}
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)
			err := ip.Publish(plugin.SnapGOBContentType, buf.Bytes(), config)
			So(err, ShouldNotBeNil)
		})

		Convey("Publish multiple metrics", func() {
			tags := map[string]string{core.STD_TAG_PLUGIN_RUNNING_ON: "hostname", "experimentId": "103", "year": "2016"}
			metrics := []plugin.MetricType{
				*plugin.NewMetricType(core.NewNamespace("integer"), time.Now(), tags, "int", 101),
				*plugin.NewMetricType(core.NewNamespace("float"), time.Now(), tags, "float64", 5.789),
				*plugin.NewMetricType(core.NewNamespace("string"), time.Now(), tags, "string", "test"),
				*plugin.NewMetricType(core.NewNamespace("boolean"), time.Now(), tags, "boolean", true),
				*plugin.NewMetricType(core.NewNamespace("test-123"), time.Now(), tags, "int", -101),
			}
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)
			err := ip.Publish(plugin.SnapGOBContentType, buf.Bytes(), config)
			So(err, ShouldBeNil)
		})

		Convey("Publish multiple metrics while having ssl options set", func() {
			tags := map[string]string{core.STD_TAG_PLUGIN_RUNNING_ON: "hostname", "ssl": "108"}
			config[sslOptionsRuleKey] = ctypes.ConfigValueBool{Value: true}
			config[usernameRuleKey] = ctypes.ConfigValueStr{Value: username}
			config[passwordRuleKey] = ctypes.ConfigValueStr{Value: password}
			config[enableServerCertVerRuleKey] = ctypes.ConfigValueBool{Value: enableServerCertVerification}
			config[caPathRuleKey] = ctypes.ConfigValueStr{Value: ""}
			config[certPathRuleKey] = ctypes.ConfigValueStr{Value: ""}
			config[keyPathRuleKey] = ctypes.ConfigValueStr{Value: ""}

			metrics := []plugin.MetricType{
				*plugin.NewMetricType(core.NewNamespace("integer"), time.Now(), tags, "int", 101),
				*plugin.NewMetricType(core.NewNamespace("float"), time.Now(), tags, "float64", 5.789),
				*plugin.NewMetricType(core.NewNamespace("string"), time.Now(), tags, "string", "test"),
				*plugin.NewMetricType(core.NewNamespace("boolean"), time.Now(), tags, "boolean", true),
				*plugin.NewMetricType(core.NewNamespace("test-123"), time.Now(), tags, "int", -101),
			}
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)
			err := ip.Publish(plugin.SnapGOBContentType, buf.Bytes(), config)
			So(err, ShouldBeNil)
		})

	})
}
