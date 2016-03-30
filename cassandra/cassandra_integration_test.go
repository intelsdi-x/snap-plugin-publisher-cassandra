// +build integration

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

func TestCassandraPublish(t *testing.T) {
	config := make(map[string]ctypes.ConfigValue)
	ip := NewCassandraPublisher()

	Convey("snap plugin CassandraDB integration testing with Cassandra", t, func() {
		var buf bytes.Buffer

		hostip := os.Getenv("SNAP_CASSANDRA_HOST")
		if len(hostip) == 0 {
			log.Fatal("SNAP_CASSANDRA_HOST is not set")
		}

		config["server"] = ctypes.ConfigValueStr{Value: hostip}

		tags := map[string]string{"zone": "red"}
		labels := []core.Label{core.Label{Index: 1, Name: "node"}}

		Convey("Publish integer metric", func() {
			metrics := []plugin.PluginMetricType{
				*plugin.NewPluginMetricType([]string{"foo"}, time.Now(), hostip, tags, labels, 99),
			}
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)
			err := ip.Publish(plugin.SnapGOBContentType, buf.Bytes(), config)
			So(err, ShouldBeNil)
		})

		Convey("Publish float metric", func() {
			metrics := []plugin.PluginMetricType{
				*plugin.NewPluginMetricType([]string{"bar"}, time.Now(), hostip, tags, labels, 3.141),
			}
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)
			err := ip.Publish(plugin.SnapGOBContentType, buf.Bytes(), config)
			So(err, ShouldBeNil)
		})

		Convey("Publish string metric", func() {
			metrics := []plugin.PluginMetricType{
				*plugin.NewPluginMetricType([]string{"qux"}, time.Now(), hostip, tags, labels, "bar"),
			}
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)
			err := ip.Publish(plugin.SnapGOBContentType, buf.Bytes(), config)
			So(err, ShouldBeNil)
		})

		Convey("Publish boolean metric", func() {
			metrics := []plugin.PluginMetricType{
				*plugin.NewPluginMetricType([]string{"baz"}, time.Now(), hostip, tags, labels, true),
			}
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)
			err := ip.Publish(plugin.SnapGOBContentType, buf.Bytes(), config)
			So(err, ShouldBeNil)
		})

		Convey("Publish map metric", func() {
			metrics := []plugin.PluginMetricType{
				*plugin.NewPluginMetricType([]string{"invalid/data/type"}, time.Now(), hostip, tags, labels, map[string]string{"foo": "bar"}),
			}
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)
			err := ip.Publish(plugin.SnapGOBContentType, buf.Bytes(), config)
			So(err, ShouldNotBeNil)
		})

		Convey("Publish multiple metrics", func() {
			metrics := []plugin.PluginMetricType{
				*plugin.NewPluginMetricType([]string{"integer"}, time.Now(), hostip, tags, labels, 101),
				*plugin.NewPluginMetricType([]string{"float"}, time.Now(), hostip, tags, labels, 5.789),
				*plugin.NewPluginMetricType([]string{"string"}, time.Now(), hostip, tags, labels, "test"),
				*plugin.NewPluginMetricType([]string{"boolean"}, time.Now(), hostip, tags, labels, true),
				*plugin.NewPluginMetricType([]string{"test-123"}, time.Now(), hostip, tags, labels, -101),
			}
			buf.Reset()
			enc := gob.NewEncoder(&buf)
			enc.Encode(metrics)
			err := ip.Publish(plugin.SnapGOBContentType, buf.Bytes(), config)
			So(err, ShouldBeNil)
		})
	})
}
