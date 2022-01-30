// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.
package rkpostgres

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

func TestRegisterPostgresEntry(t *testing.T) {
	// without options
	entry := RegisterPostgresEntry()

	assert.NotEmpty(t, entry.GetName())
	assert.NotEmpty(t, entry.GetType())
	assert.NotEmpty(t, entry.GetDescription())
	assert.NotEmpty(t, entry.String())
	assert.Equal(t, "postgres", entry.User)
	assert.Equal(t, "pass", entry.pass)
	assert.Equal(t, "localhost:5432", entry.Addr)
	assert.Empty(t, entry.GormDbMap)
	assert.Empty(t, entry.GormConfigMap)

	// remove entry
	rkentry.GlobalAppCtx.RemoveEntry(entry.GetName())

	// with options
	entry = RegisterPostgresEntry(
		WithName("ut-entry"),
		WithDescription("ut-entry"),
		WithUser("ut-user"),
		WithPass("ut-pass"),
		WithAddr("ut-addr"),
		WithDatabase("ut-database", true, false, false))

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

func TestPostgresEntry_IsHealthy(t *testing.T) {
	// test with dry run enabled
	entry := RegisterPostgresEntry()
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

func TestGetPostgresEntry(t *testing.T) {
	// expect nil
	assert.Nil(t, GetPostgresEntry("not-exist"))

	// with invalid entry
	assert.Nil(t, GetPostgresEntry(rkentry.GlobalAppCtx.GetAppInfoEntry().GetName()))

	entry := RegisterPostgresEntry()
	defer rkentry.GlobalAppCtx.RemoveEntry(entry.GetName())
	// happy case
	assert.Equal(t, entry, GetPostgresEntry(entry.GetName()))
}

func TestRegisterPostgresEntriesWithConfig(t *testing.T) {
	bootConfigStr := `
postgres:
  - name: user-db
    enabled: true
    locale: "*::*::*::*"
    addr: "localhost:5432"
    user: postgres
    pass: pass
    logger:
      level: warn
      encoding: json
      outputPaths: [ "pg/log" ]
    database:
      - name: user
        autoCreate: true
        dryRun: false
        preferSimpleProtocol: false
        params: []
`

	tempDir := path.Join(t.TempDir(), "boot.yaml")
	assert.Nil(t, ioutil.WriteFile(tempDir, []byte(bootConfigStr), os.ModePerm))

	entries := RegisterPostgresEntriesWithConfig(tempDir)

	assert.NotEmpty(t, entries)

	rkentry.GlobalAppCtx.RemoveEntry("user-db")
}

func TestPostgresEntry_Bootstrap(t *testing.T) {
	defer assertPanic(t)

	entry := RegisterPostgresEntry(
		WithDatabase("ut-database", false, true, false))
	entry.Bootstrap(context.TODO())

	assert.NotNil(t, entry.GetDB("ut-database"))
	assert.True(t, entry.IsHealthy())
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
