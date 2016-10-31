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
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core/ctypes"
	"github.com/pkg/errors"
)

const (
	name       = "cassandra"
	version    = 2
	pluginType = plugin.PublisherPluginType

	serverAddrRuleKey         = "server"
	serverAddrRuleRequired    = true
	serverAddrRuleDescription = "Cassandra server"
	serverErr                 = "Server not found"
	invalidServer             = "Invalid server"

	sslOptionsRuleKey         = "ssl"
	sslOptionsRuleRequired    = false
	sslOptionsRuleDefault     = false
	sslOptionsRuleDescription = "Not required, if true, use ssl options to connect to the Cassandra, default: false"

	stringRuleDefaultValue  = ""
	usernameRuleKey         = "username"
	usernameRuleRequired    = false
	usernameRuleDescription = "Name of a user used to authenticate to Cassandra"

	passwordRuleKey         = "password"
	passwordRuleRequired    = false
	passwordRuleDescription = "Password used to authenticate to the Cassandra"

	keyPathRuleKey         = "keyPath"
	keyPathRuleRequired    = false
	keyPathRuleDescription = "Path to the private key for the Cassandra client"

	certPathRuleKey         = "certPath"
	certPathRuleRequired    = false
	certPathRuleDescription = "Path to the self signed certificate for the Cassandra client"

	caPathRuleKey         = "caPath"
	caPathRuleRequired    = false
	caPathRuleDescription = "Path to the CA certificate for the Cassandra server"

	enableServerCertVerRuleKey         = "serverCertVerification"
	enableServerCertVerRuleRequired    = false
	enableServerCertVerRuleDefault     = true
	enableServerCertVerRuleDescription = "If true, verify a hostname and a server key, default: true"

	timeoutRuleKey         = "timeout"
	timeoutRuleRequired    = false
	timeoutRuleDefault     = 10
	timeoutRuleDescription = "Connection timeout in seconds, defaul: 10s"
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

type SslOptions struct {
	username                     string
	password                     string
	keyPath                      string
	certPath                     string
	caPath                       string
	enableServerCertVerification bool
	timeout                      time.Duration
}

// GetConfigPolicy returns plugin mandatory fields as the config policy
func (cas *CassandraPublisher) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {
	cp := cpolicy.New()
	config := cpolicy.NewPolicyNode()

	serverAddrRule, err := cpolicy.NewStringRule(serverAddrRuleKey, serverAddrRuleRequired)
	handleErr(err)
	serverAddrRule.Description = serverAddrRuleDescription
	config.Add(serverAddrRule)

	useSslOptionsRule, err := cpolicy.NewBoolRule(sslOptionsRuleKey, sslOptionsRuleRequired, sslOptionsRuleDefault)
	handleErr(err)
	useSslOptionsRule.Description = sslOptionsRuleDescription
	config.Add(useSslOptionsRule)

	usernameRule, err := cpolicy.NewStringRule(usernameRuleKey, usernameRuleRequired, stringRuleDefaultValue)
	handleErr(err)
	usernameRule.Description = usernameRuleDescription
	config.Add(usernameRule)

	passwordRule, err := cpolicy.NewStringRule(passwordRuleKey, passwordRuleRequired, stringRuleDefaultValue)
	handleErr(err)
	passwordRule.Description = passwordRuleDescription
	config.Add(passwordRule)

	keyPathRule, err := cpolicy.NewStringRule(keyPathRuleKey, keyPathRuleRequired, stringRuleDefaultValue)
	handleErr(err)
	keyPathRule.Description = keyPathRuleDescription
	config.Add(keyPathRule)

	certPathRule, err := cpolicy.NewStringRule(certPathRuleKey, certPathRuleRequired, stringRuleDefaultValue)
	handleErr(err)
	certPathRule.Description = certPathRuleDescription
	config.Add(certPathRule)

	caPathRule, err := cpolicy.NewStringRule(caPathRuleKey, caPathRuleRequired, stringRuleDefaultValue)
	handleErr(err)
	caPathRule.Description = caPathRuleDescription
	config.Add(caPathRule)

	enableServerCertVerRule, err := cpolicy.NewBoolRule(
		enableServerCertVerRuleKey, enableServerCertVerRuleRequired, enableServerCertVerRuleDefault)
	handleErr(err)
	enableServerCertVerRule.Description = enableServerCertVerRuleDescription
	config.Add(enableServerCertVerRule)

	timeout, err := cpolicy.NewIntegerRule(
		timeoutRuleKey, timeoutRuleRequired, timeoutRuleDefault)
	handleErr(err)
	timeout.Description = timeoutRuleDescription
	config.Add(timeout)

	cp.Add([]string{""}, config)
	return cp, nil
}

// Publish publishes metric data to Cassandra
func (cas *CassandraPublisher) Publish(contentType string, content []byte, config map[string]ctypes.ConfigValue) error {
	logger := getLogger(config)
	var metrics []plugin.MetricType

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

	useSslOptions := getBoolValueForKey(config, sslOptionsRuleKey)
	var sslOptions *SslOptions
	var err error
	if useSslOptions {
		sslOptions, err = getSslOptions(config)
		if err != nil {
			logger.Fatal(err)
			return err
		}
	}
	// Only initialize client once if possible
	if cas.client == nil {
		cas.client = NewCassaClient(getServerAddress(config), sslOptions)
	}
	return cas.client.saveMetrics(metrics)
}

// Close closes the Cassandra client session
func (cas *CassandraPublisher) Close() {
	if cas.client != nil {
		cas.client.session.Close()
	}
}

func getBoolValueForKey(cfg map[string]ctypes.ConfigValue, key string) bool {
	if cfg == nil {
		log.Fatal("Configuration of a plugin not found")
	}
	configElem := cfg[key]

	if configElem == nil || configElem.Type() != "bool" {
		log.Fatalf("Valid configuration not found for a key %s", key)
	}

	var value bool
	switch configElem.Type() {
	case "bool":
		value = configElem.(ctypes.ConfigValueBool).Value
	default:
		log.Fatalf("Value for a key %s should have a bool type", key)
	}
	return value
}

func getStringValueForKey(cfg map[string]ctypes.ConfigValue, key string) string {
	if cfg == nil {
		log.Fatal("Configuration of a plugin not found")
	}
	configElem := cfg[key]
	if configElem == nil || configElem.Type() != "string" {
		log.Fatalf("Valid configuration not found for a key %s", key)
	}

	var value string
	switch configElem.Type() {
	case "string":
		value = configElem.(ctypes.ConfigValueStr).Value
	default:
		log.Fatalf("Value for a key %s should have a string type", key)
	}

	return value
}

func getServerAddress(cfg map[string]ctypes.ConfigValue) string {
	if cfg == nil {
		log.Fatal(serverErr)
	}
	server := cfg[serverAddrRuleKey]

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

func getTimeout(cfg map[string]ctypes.ConfigValue) time.Duration {
	if cfg == nil {
		log.Fatal("Configuration of a plugin not found")
	}
	timeout := cfg[timeoutRuleKey]

	if timeout == nil || timeout.Type() != "integer" {
		log.Fatalf("Valid configuration not found for a key %s", timeoutRuleKey)
	}

	var result int
	switch timeout.Type() {
	case "integer":
		result = timeout.(ctypes.ConfigValueInt).Value
	default:
		log.Fatalf("Value for a key %s should have an int type", timeoutRuleKey)
	}
	return time.Duration(result) * time.Second
}

func getSslOptions(cfg map[string]ctypes.ConfigValue) (*SslOptions, error) {
	options := SslOptions{
		username: getStringValueForKey(cfg, usernameRuleKey),
		password: getStringValueForKey(cfg, passwordRuleKey),
		keyPath:  getStringValueForKey(cfg, keyPathRuleKey),
		certPath: getStringValueForKey(cfg, certPathRuleKey),
		caPath:   getStringValueForKey(cfg, caPathRuleKey),
		enableServerCertVerification: getBoolValueForKey(cfg, enableServerCertVerRuleKey),
		timeout: getTimeout(cfg),
	}
	// Check whether necessary options were set.
	if options.keyPath == "" || options.certPath == "" || options.caPath == "" {
		return &options, errors.Errorf("While using ssl, %s, %s and %s have to be specified in the plugin config",
			keyPathRuleKey, certPathRuleKey, caPathRuleKey)
	}
	return &options, nil
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
