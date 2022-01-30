// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.
package rkclickhouse

import (
	"context"
	"github.com/rookie-ninja/rk-entry/entry"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestRegisterClickHouseEntry(t *testing.T) {
	// without options
	entry := RegisterClickHouseEntry()

	assert.NotEmpty(t, entry.GetName())
	assert.NotEmpty(t, entry.GetType())
	assert.NotEmpty(t, entry.GetDescription())
	assert.NotEmpty(t, entry.String())
	assert.Equal(t, "default", entry.User)
	assert.Equal(t, "", entry.pass)
	assert.Equal(t, "localhost:9000", entry.Addr)
	assert.Empty(t, entry.GormDbMap)
	assert.Empty(t, entry.GormConfigMap)

	// remove entry
	rkentry.GlobalAppCtx.RemoveEntry(entry.GetName())

	// with options
	entry = RegisterClickHouseEntry(
		WithName("ut-entry"),
		WithDescription("ut-entry"),
		WithUser("ut-user"),
		WithPass("ut-pass"),
		WithAddr("ut-addr"),
		WithDatabase("ut-database", true, false))

	assert.Equal(t, "ut-entry", entry.GetName())
	assert.NotEmpty(t, entry.GetType())
	assert.Equal(t, "ut-entry", entry.GetDescription())
	assert.NotEmpty(t, entry.String())
	assert.Equal(t, "ut-user", entry.User)
	assert.Equal(t, "ut-pass", entry.pass)
	assert.Equal(t, "ut-addr", entry.Addr)
	assert.Empty(t, entry.GormDbMap)
	assert.NotEmpty(t, entry.GormConfigMap)

	// remove entry
	rkentry.GlobalAppCtx.RemoveEntry(entry.GetName())
}

func TestClickHouseEntry_IsHealthy(t *testing.T) {
	// test with dry run enabled
	entry := RegisterClickHouseEntry()
	entry.Bootstrap(context.TODO())

	assert.True(t, entry.IsHealthy())

	defer rkentry.GlobalAppCtx.RemoveEntry(entry.GetName())
	defer entry.Interrupt(context.TODO())
}

func TestCopyZapLoggerConfig(t *testing.T) {
	src := &zap.Config{
		Level:             zap.NewAtomicLevel(),
		Development:       true,
		DisableCaller:     true,
		DisableStacktrace: true,
		Sampling:          &zap.SamplingConfig{},
		Encoding:          "ut-encoding",
		EncoderConfig:     zap.NewDevelopmentEncoderConfig(),
		OutputPaths:       []string{},
		ErrorOutputPaths:  []string{},
		InitialFields:     make(map[string]interface{}),
	}

	target := copyZapLoggerConfig(src)

	assert.Equal(t, src.Level, target.Level)
	assert.Equal(t, src.Development, target.Development)
	assert.Equal(t, src.DisableCaller, target.DisableCaller)
	assert.Equal(t, src.DisableStacktrace, target.DisableStacktrace)
	assert.Equal(t, src.Sampling, target.Sampling)
	assert.Equal(t, src.Encoding, target.Encoding)
	assert.Equal(t, src.OutputPaths, target.OutputPaths)
	assert.Equal(t, src.ErrorOutputPaths, target.ErrorOutputPaths)
	assert.Equal(t, src.InitialFields, target.InitialFields)
}

func TestGetClickHouseEntry(t *testing.T) {
	// expect nil
	assert.Nil(t, GetClickHouseEntry("not-exist"))

	// with invalid entry
	assert.Nil(t, GetClickHouseEntry(rkentry.GlobalAppCtx.GetAppInfoEntry().GetName()))

	entry := RegisterClickHouseEntry()
	defer rkentry.GlobalAppCtx.RemoveEntry(entry.GetName())
	// happy case
	assert.Equal(t, entry, GetClickHouseEntry(entry.GetName()))
}

func TestRegisterClickHouseEntriesWithConfig(t *testing.T) {
	bootConfigStr := `
clickHouse:
  - name: user-db
    enabled: true
    locale: "*::*::*::*"
    addr: "localhost:9000"
    user: default
    pass: ""
    logger:
      level: warn
      encoding: json
      outputPaths: [ "clickhouse/log" ]
    database:
      - name: user
        autoCreate: true
        dryRun: false
        params: []
`

	tempDir := path.Join(t.TempDir(), "boot.yaml")
	assert.Nil(t, ioutil.WriteFile(tempDir, []byte(bootConfigStr), os.ModePerm))

	entries := RegisterClickHouseEntriesWithConfig(tempDir)

	assert.NotEmpty(t, entries)

	rkentry.GlobalAppCtx.RemoveEntry("user-db")
}

func TestClickHouseEntry_Bootstrap(t *testing.T) {
	defer assertPanic(t)

	entry := RegisterClickHouseEntry(
		WithDatabase("ut-database", false, true))
	entry.Bootstrap(context.TODO())

	assert.NotNil(t, entry.GetDB("ut-database"))
	assert.True(t, entry.IsHealthy())
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
