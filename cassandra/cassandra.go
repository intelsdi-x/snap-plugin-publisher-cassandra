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
	"fmt"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core/ctypes"
)

const (
	name       = "cassandra"
	version    = 1
	pluginType = plugin.PublisherPluginType

	serverAddr    = "server"
	serverErr     = "Server not found"
	invalidServer = "Invalid server"
)

// Meta returns a plugin meta data
func Meta() *plugin.PluginMeta {
	return plugin.NewPluginMeta(name, version, pluginType, []string{plugin.SnapGOBContentType},
		[]string{plugin.SnapGOBContentType}, plugin.RoutingStrategy(plugin.StickyRouting), plugin.ConcurrencyCount(1))
}

// NewCassandraPublisher returns an instance of the Cassandra publisher
// Client is not initiated until the first data publish happends.
func NewCassandraPublisher() *CassandraPublisher {
	return &CassandraPublisher{}
}

// CassandraPublisher defines Cassandra publisher
type CassandraPublisher struct {
	client *cassaClient
}

// GetConfigPolicy returns plugin mandatory fields as the config policy
func (cas *CassandraPublisher) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {
	cp := cpolicy.New()
	config := cpolicy.NewPolicyNode()

	r1, err := cpolicy.NewStringRule(serverAddr, true)
	handleErr(err)
	r1.Description = "Cassandra server"
	config.Add(r1)

	cp.Add([]string{""}, config)
	return cp, nil
}

// Publish publishes metric data to Cassandra
func (cas *CassandraPublisher) Publish(contentType string, content []byte, config map[string]ctypes.ConfigValue) error {
	logger := getLogger(config)
	var metrics []plugin.PluginMetricType

	switch contentType {
	case plugin.SnapGOBContentType:
		dec := gob.NewDecoder(bytes.NewBuffer(content))
		if err := dec.Decode(&metrics); err != nil {
			logger.WithFields(log.Fields{
				"err": err,
			}).Error("decoding error")
			return err
		}
	default:
		logger.Errorf("unknown content type '%v'", contentType)
		return fmt.Errorf("Unknown content type '%s'", contentType)
	}

	// Only initialize client once if possible
	if cas.client == nil {
		cas.client = NewCassaClient(getServerAddress(config))
	}
	return cas.client.saveMetrics(metrics)
}

// Close closes the Cassandra client session
func (cas *CassandraPublisher) Close() {
	cas.client.session.Close()
}

func getServerAddress(cfg map[string]ctypes.ConfigValue) string {
	if cfg == nil {
		log.Fatal(serverErr)
	}
	server := cfg[serverAddr]

	if server == nil || server.Type() != "string" {
		log.Fatal(serverErr)
	}

	result := "localhost"
	switch server.Type() {
	case "string":
		result = server.(ctypes.ConfigValueStr).Value
	default:
		log.Fatal(invalidServer)
	}
	return result
}

func handleErr(e error) {
	if e != nil {
		log.Fatal(e.Error())
	}
}

func getLogger(config map[string]ctypes.ConfigValue) *log.Entry {
	logger := log.WithFields(log.Fields{
		"plugin-name":    name,
		"plugin-version": version,
		"plugin-type":    pluginType.String(),
	})

	// default
	log.SetLevel(log.WarnLevel)

	if debug, ok := config["debug"]; ok {
		switch v := debug.(type) {
		case ctypes.ConfigValueBool:
			if v.Value {
				log.SetLevel(log.DebugLevel)
				return logger
			}
		default:
			logger.WithFields(log.Fields{
				"field":         "debug",
				"type":          v,
				"expected type": "ctypes.ConfigValueBool",
			}).Error("invalid config type")
		}
	}

	if loglevel, ok := config["log-level"]; ok {
		switch v := loglevel.(type) {
		case ctypes.ConfigValueStr:
			switch strings.ToLower(v.Value) {
			case "warn":
				log.SetLevel(log.WarnLevel)
			case "error":
				log.SetLevel(log.ErrorLevel)
			case "debug":
				log.SetLevel(log.DebugLevel)
			case "info":
				log.SetLevel(log.InfoLevel)
			default:
				log.WithFields(log.Fields{
					"value":             strings.ToLower(v.Value),
					"acceptable values": "warn, error, debug, info",
				}).Warn("invalid config value")
			}
		default:
			logger.WithFields(log.Fields{
				"field":         "log-level",
				"type":          v,
				"expected type": "ctypes.ConfigValueStr",
			}).Error("invalid config type")
		}
	}

	return logger
}
