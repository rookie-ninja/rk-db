// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.

package rkmongo

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"
)

const yamlStr = `
appName: "appName"
auth:
  mechanism: "mechanism"
  mechanismProperties:
    prop: "prop"
  source: "source"
  username: "username"
  password: "password"
  passwordSet: true
connectTimeoutMs: 1
compressors: ["compressor"]
direct: true
disableOCSPEndpointCheck: true
heartbeatIntervalMs: 1
hosts: ["host"]
loadBalanced: true
localThresholdMs: 1
maxConnIdleTimeMs: 1
maxPoolSize: 1
minPoolSize: 1
maxConnecting: 1
replicaSet: "replicaSet"
retryReads: true
retryWrites: true
serverApiOptions:
  version: "version"
  strict: true
  deprecationErrors: true
serverSelectionTimeoutMs: 1
socketTimeoutMs: 1
srvMaxHosts: 1
srvServiceName: "srvServiceName"
zlibLevel: 1
zstdLevel: 1
`

func TestToClientOptions(t *testing.T) {
	defer assertNotPanic(t)

	// with nil
	assert.NotNil(t, ToClientOptions(nil))

	// with nil element
	assert.NotNil(t, ToClientOptions(&BootConfigMongo{}))

	// happy case
	config := &BootConfigMongo{}
	assert.Nil(t, yaml.Unmarshal([]byte(yamlStr), config))
	opts := ToClientOptions(config)
	assert.NotNil(t, opts)

	assert.Equal(t, "appName", *opts.AppName)
	assert.Equal(t, "mechanism", opts.Auth.AuthMechanism)
	assert.NotEmpty(t, opts.Auth.AuthMechanismProperties)
	assert.Equal(t, "source", opts.Auth.AuthSource)
	assert.Equal(t, "username", opts.Auth.Username)
	assert.Equal(t, "password", opts.Auth.Password)
	assert.True(t, opts.Auth.PasswordSet)
	assert.Equal(t, time.Millisecond, *opts.ConnectTimeout)
	assert.Len(t, opts.Compressors, 1)
	assert.True(t, *opts.Direct)
	assert.True(t, *opts.DisableOCSPEndpointCheck)
	assert.Len(t, opts.Hosts, 1)
	assert.True(t, *opts.LoadBalanced)
	assert.Equal(t, time.Millisecond, *opts.LocalThreshold)
	assert.Equal(t, time.Millisecond, *opts.MaxConnIdleTime)
	assert.Equal(t, uint64(1), *opts.MaxPoolSize)
	assert.Equal(t, uint64(1), *opts.MinPoolSize)
	assert.Equal(t, uint64(1), *opts.MaxConnecting)
	assert.Equal(t, "replicaSet", *opts.ReplicaSet)
	assert.True(t, *opts.RetryReads)
	assert.True(t, *opts.RetryWrites)
	assert.Equal(t, "version", string(opts.ServerAPIOptions.ServerAPIVersion))
	assert.True(t, *opts.ServerAPIOptions.Strict)
	assert.True(t, *opts.ServerAPIOptions.DeprecationErrors)
	assert.Equal(t, time.Millisecond, *opts.ServerSelectionTimeout)
	assert.Equal(t, time.Millisecond, *opts.SocketTimeout)
	assert.Equal(t, 1, *opts.SRVMaxHosts)
	assert.Equal(t, "srvServiceName", *opts.SRVServiceName)
	assert.Equal(t, 1, *opts.ZlibLevel)
	assert.Equal(t, 1, *opts.ZstdLevel)
}

func TestRegisterMongoEntriesFromConfig(t *testing.T) {
	defer assertNotPanic(t)

	fullYamlStr := `
---
mongo:
  - name: "mongo"
    enabled: true
    description: "description"
    simpleURI: ""
    database:
      - name: "database"
%s
`

	tempDir := path.Join(t.TempDir(), "boot.yaml")
	assert.Nil(t, ioutil.WriteFile(tempDir, []byte(fmt.Sprintf(fullYamlStr, yamlStr)), os.ModePerm))

	entries := RegisterMongoEntryYAML([]byte(fmt.Sprintf(fullYamlStr, yamlStr)))

	assert.Len(t, entries, 1)

	entry := entries["mongo"].(*MongoEntry)
	assert.NotNil(t, entry)
	assert.NotEmpty(t, entry.GetName())
	assert.NotEmpty(t, entry.GetType())
	assert.NotEmpty(t, entry.GetDescription())
	assert.NotEmpty(t, entry.String())
	assert.Nil(t, entry.GetMongoClient())
	assert.Nil(t, entry.GetMongoDB("database"))
	assert.Nil(t, entry.GetMongoClient())
	assert.NotNil(t, entry.GetMongoClientOptions())
}

func TestMongoEntry_Bootstrap(t *testing.T) {
	defer assertNotPanic(t)

	entry := RegisterMongoEntry(
		WithDatabase("database"))
	assert.NotNil(t, entry)
	entry.Bootstrap(context.TODO())

	assert.NotNil(t, entry.GetMongoClient())
	assert.NotNil(t, entry.GetMongoDB("database"))
}

func TestMongoEntry_Interrupt(t *testing.T) {
	defer assertNotPanic(t)

	// without bootstrap
	entry := RegisterMongoEntry(
		WithDatabase("database"))
	assert.NotNil(t, entry)
	entry.Interrupt(context.TODO())

	// happy case
	entry.Bootstrap(context.TODO())
	entry.Interrupt(context.TODO())
}

func assertNotPanic(t *testing.T) {
	if r := recover(); r != nil {
		// Expect panic to be called with non nil error
		assert.True(t, false)
	} else {
		// This should never be called in case of a bug
		assert.True(t, true)
	}
}

func assertPanic(t *testing.T) {
	if r := recover(); r != nil {
		// Expect panic to be called with non nil error
		assert.True(t, true)
	} else {
		// This should never be called in case of a bug
		assert.True(t, false)
	}
}
