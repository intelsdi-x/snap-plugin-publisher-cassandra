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
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core/ctypes"
)

const (
	name       = "cassandra"
	version    = 2
	pluginType = plugin.PublisherPluginType

	serverAddrRuleKey          = "server"
	sslOptionsRuleKey          = "ssl"
	usernameRuleKey            = "username"
	passwordRuleKey            = "password"
	keyPathRuleKey             = "keyPath"
	certPathRuleKey            = "certPath"
	caPathRuleKey              = "caPath"
	enableServerCertVerRuleKey = "serverCertVerification"
	timeoutRuleKey             = "timeout"
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

	serverAddrRule, err := cpolicy.NewStringRule("server", true)
	handleErr(err)
	serverAddrRule.Description = "Cassandra server"
	config.Add(serverAddrRule)

	useSslOptionsRule, err := cpolicy.NewBoolRule("ssl", false, false)
	handleErr(err)
	useSslOptionsRule.Description = "Not required, if true, use ssl options to connect to the Cassandra, default: false"
	config.Add(useSslOptionsRule)

	usernameRule, err := cpolicy.NewStringRule("username", false, "")
	handleErr(err)
	usernameRule.Description = "Name of a user used to authenticate to Cassandra"
	config.Add(usernameRule)

	passwordRule, err := cpolicy.NewStringRule("password", false, "")
	handleErr(err)
	passwordRule.Description = "Password used to authenticate to the Cassandra"
	config.Add(passwordRule)

	keyPathRule, err := cpolicy.NewStringRule("keyPath", false, "")
	handleErr(err)
	keyPathRule.Description = "Path to the private key for the Cassandra client"
	config.Add(keyPathRule)

	certPathRule, err := cpolicy.NewStringRule("certPath", false, "")
	handleErr(err)
	certPathRule.Description = "Path to the self signed certificate for the Cassandra client"
	config.Add(certPathRule)

	caPathRule, err := cpolicy.NewStringRule("caPath", false, "")
	handleErr(err)
	caPathRule.Description = "Path to the CA certificate for the Cassandra server"
	config.Add(caPathRule)

	enableServerCertVerRule, err := cpolicy.NewBoolRule(
		"serverCertVerification", false, true)
	handleErr(err)
	enableServerCertVerRule.Description = "If true, verify a hostname and a server key, default: true"
	config.Add(enableServerCertVerRule)

	timeout, err := cpolicy.NewIntegerRule(
		"timeout", false, 0)
	handleErr(err)
	timeout.Description = "Connection timeout in seconds, defaul: 0s"
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

	useSslOptions, ok := getValueForKey(config, sslOptionsRuleKey).(bool)
	checkAssertion(ok, sslOptionsRuleKey)

	var sslOptions *sslOptions
	var err error
	if useSslOptions {
		sslOptions, err = getSslOptions(config)
		if err != nil {
			logger.Error(err)
			return err
		}
	}

	timeout, ok := getValueForKey(config, timeoutRuleKey).(int)
	checkAssertion(ok, timeoutRuleKey)
	serverAddr, ok := getValueForKey(config, serverAddrRuleKey).(string)
	checkAssertion(ok, serverAddrRuleKey)
	// Only initialize client once if possible
	if cas.client == nil {
		cas.client = NewCassaClient(serverAddr, sslOptions,
			time.Duration(timeout)*time.Second)
	}
	return cas.client.saveMetrics(metrics)
}

// Close closes the Cassandra client session
func (cas *CassandraPublisher) Close() {
	if cas.client != nil {
		cas.client.session.Close()
	}
}

func getValueForKey(cfg map[string]ctypes.ConfigValue, key string) interface{} {
	if cfg == nil {
		log.Fatal("Configuration of a plugin not found")
	}
	configElem := cfg[key]

	if configElem == nil {
		log.Fatalf("Valid configuration not found for a key %s", key)
	}
	var value interface{}
	switch configElem.Type() {
	case "bool":
		value = configElem.(ctypes.ConfigValueBool).Value
	case "string":
		value = configElem.(ctypes.ConfigValueStr).Value
	case "integer":
		value = configElem.(ctypes.ConfigValueInt).Value
	default:
		log.Fatalf("Proper value type not found for a key %s", key)
	}
	return value
}

func getSslOptions(cfg map[string]ctypes.ConfigValue) (*sslOptions, error) {
	username, ok := getValueForKey(cfg, usernameRuleKey).(string)
	checkAssertion(ok, usernameRuleKey)
	password, ok := getValueForKey(cfg, passwordRuleKey).(string)
	checkAssertion(ok, passwordRuleKey)
	keyPath, ok := getValueForKey(cfg, keyPathRuleKey).(string)
	checkAssertion(ok, keyPathRuleKey)
	certPath, ok := getValueForKey(cfg, certPathRuleKey).(string)
	checkAssertion(ok, certPathRuleKey)
	caPath, ok := getValueForKey(cfg, caPathRuleKey).(string)
	checkAssertion(ok, caPathRuleKey)
	enableServerCertVerification, ok := getValueForKey(cfg, enableServerCertVerRuleKey).(bool)
	checkAssertion(ok, enableServerCertVerRuleKey)

	options := sslOptions{
		username: username,
		password: password,
		keyPath:  keyPath,
		certPath: certPath,
		caPath:   caPath,
		enableServerCertVerification: enableServerCertVerification,
	}
	// Check whether necessary options were set.
	if options.keyPath == "" || options.certPath == "" || options.caPath == "" {
		errorMsg := fmt.Sprintf("While using ssl, %s, %s and %s have to be specified in the plugin config",
			keyPathRuleKey, certPathRuleKey, caPathRuleKey)
		return &options, errors.New(errorMsg)
	}
	return &options, nil
}

func handleErr(e error) {
	if e != nil {
		log.Fatal(e.Error())
	}
}

func checkAssertion(ok bool, key string) {
	if !ok {
		errorMsg := fmt.Sprintf("Invalid data type for a key %s", sslOptionsRuleKey)
		err := errors.New(errorMsg)
		log.Fatal(err)
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
