// +build unit

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
	"reflect"
	"testing"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core/ctypes"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCassandraDBPlugin(t *testing.T) {
	const (
		serverAddress                = "127.0.0.1"
		sslOptionsFlag               = true
		username                     = "username"
		password                     = "password"
		path                         = "/some/path"
		timeout                      = 10
		enableServerCertVerification = false
	)
	Convey("Meta should return metadata for the plugin", t, func() {
		meta := Meta()
		So(meta.Name, ShouldResemble, name)
		So(meta.Version, ShouldResemble, version)
		So(meta.Type, ShouldResemble, plugin.PublisherPluginType)
	})

	Convey("Create CassandraPublisher", t, func() {
		ip := NewCassandraPublisher()
		Convey("So ip should not be nil", func() {
			So(ip, ShouldNotBeNil)
		})
		Convey("So ip should be of cassandraPublisher type", func() {
			So(ip, ShouldHaveSameTypeAs, &CassandraPublisher{})
		})
		configPolicy, err := ip.GetConfigPolicy()
		Convey("ip.GetConfigPolicy() should return a config policy", func() {
			Convey("So config policy should not be nil", func() {
				So(configPolicy, ShouldNotBeNil)
			})
			Convey("So we should not get an err retreiving the config policy", func() {
				So(err, ShouldBeNil)
			})
			Convey("So config policy should be a cpolicy.ConfigPolicy", func() {
				So(configPolicy, ShouldHaveSameTypeAs, &cpolicy.ConfigPolicy{})
			})
			testConfig := make(map[string]ctypes.ConfigValue)

			testConfig["server"] = ctypes.ConfigValueStr{Value: serverAddress}
			cfg, errs := configPolicy.Get([]string{""}).Process(testConfig)
			Convey("So config policy should process testConfig and return a config", func() {
				So(cfg, ShouldNotBeNil)
			})
			Convey("So testConfig processing should return no errors", func() {
				So(errs.HasErrors(), ShouldBeFalse)
			})
			Convey("So getting the server address should return a proper value", func() {
				receivedServerAddress, ok := getValueForKey(testConfig, serverAddrRuleKey).(string)
				So(ok, ShouldBeTrue)
				So(receivedServerAddress, ShouldEqual, serverAddress)
				So(reflect.TypeOf(receivedServerAddress).String(), ShouldEqual, "string")
			})

			testConfig = make(map[string]ctypes.ConfigValue)
			testConfig["port"] = ctypes.ConfigValueStr{Value: "9042"}
			cfg, errs = configPolicy.Get([]string{""}).Process(testConfig)
			Convey("So config policy should not return a config after processing invalid testConfig", func() {
				So(cfg, ShouldBeNil)
			})
			Convey("So testConfig processing should return errors", func() {
				So(errs.HasErrors(), ShouldBeTrue)
			})

			// Prepare ssl options struct with expected values.
			expectedSslOptions := &sslOptions{
				username: username,
				password: password,
				certPath: path,
				caPath:   path,
				keyPath:  path,
				enableServerCertVerification: enableServerCertVerification,
			}

			// Prepare test config with ssl options.
			testConfig = make(map[string]ctypes.ConfigValue)
			testConfig[serverAddrRuleKey] = ctypes.ConfigValueStr{Value: serverAddress}
			testConfig[sslOptionsRuleKey] = ctypes.ConfigValueBool{Value: sslOptionsFlag}
			testConfig[usernameRuleKey] = ctypes.ConfigValueStr{Value: username}
			testConfig[passwordRuleKey] = ctypes.ConfigValueStr{Value: password}
			testConfig[caPathRuleKey] = ctypes.ConfigValueStr{Value: path}
			testConfig[certPathRuleKey] = ctypes.ConfigValueStr{Value: path}
			testConfig[keyPathRuleKey] = ctypes.ConfigValueStr{Value: path}
			testConfig[enableServerCertVerRuleKey] = ctypes.ConfigValueBool{Value: enableServerCertVerification}

			cfg, errs = configPolicy.Get([]string{""}).Process(testConfig)
			Convey("So config policy should return a config after processing testConfig with valid ssl options", func() {
				So(cfg, ShouldNotBeNil)
			})
			Convey("So testConfig processing should not return errors", func() {
				So(errs.HasErrors(), ShouldBeFalse)
			})

			// Get ssl options from the test config.
			receivedSslOptions, err := getSslOptions(testConfig)
			Convey("So received ssl options struct should have proper values for all keys", func() {
				So(reflect.DeepEqual(expectedSslOptions, receivedSslOptions), ShouldBeTrue)
			})
			Convey("So getting ssl options for valid config should not return any error", func() {
				So(err, ShouldBeNil)
			})

			// Prepare cluster for a given address.
			cluster := createCluster(serverAddress)
			Convey("So while creating cluster it should not be nil", func() {
				So(cluster, ShouldNotBeNil)
			})

			// 'Decorate' prepared cluster with received ssl options.
			clusterWithSslOptions := addSslOptions(cluster, receivedSslOptions)
			Convey("So after adding ssl options a cluster should have a proper ca path", func() {
				So(clusterWithSslOptions.SslOpts.CaPath, ShouldEqual, expectedSslOptions.caPath)
			})
			Convey("So after adding ssl options a cluster should have a proper cert path", func() {
				So(clusterWithSslOptions.SslOpts.CertPath, ShouldEqual, expectedSslOptions.certPath)
			})
			Convey("So after adding ssl options a cluster should have a proper key path", func() {
				So(clusterWithSslOptions.SslOpts.KeyPath, ShouldEqual, expectedSslOptions.keyPath)
			})
		})
	})
}
