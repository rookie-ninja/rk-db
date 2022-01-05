// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.
package rkmysql

import (
	"context"
	"github.com/rookie-ninja/rk-entry/entry"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
)

func TestRegisterMySqlEntry(t *testing.T) {
	// without options
	entry := RegisterMySqlEntry()

	assert.NotEmpty(t, entry.GetName())
	assert.NotEmpty(t, entry.GetType())
	assert.NotEmpty(t, entry.GetDescription())
	assert.NotEmpty(t, entry.String())
	assert.Equal(t, "root", entry.User)
	assert.Equal(t, "pass", entry.pass)
	assert.Equal(t, "tcp", entry.Protocol)
	assert.Equal(t, "localhost:3306", entry.Addr)
	assert.Empty(t, entry.GormDbMap)
	assert.Empty(t, entry.GormConfigMap)
	assert.Equal(t, EncodingConsole, entry.loggerEncoding)
	assert.Equal(t, LoggerLevelWarn, entry.loggerLevel)
	assert.Empty(t, entry.loggerOutputPath)
	assert.NotNil(t, entry.Logger)

	// remove entry
	rkentry.GlobalAppCtx.RemoveEntry(entry.GetName())

	// with options
	entry = RegisterMySqlEntry(
		WithName("ut-entry"),
		WithDescription("ut-entry"),
		WithUser("ut-user"),
		WithPass("ut-pass"),
		WithProtocol("ut-protocol"),
		WithAddr("ut-addr"),
		WithDatabase("ut-database", true, false),
		WithLoggerEncoding(EncodingJson),
		WithLoggerOutputPaths("ut-output"),
		WithLoggerLevel(LoggerLevelInfo))

	assert.Equal(t, "ut-entry", entry.GetName())
	assert.NotEmpty(t, entry.GetType())
	assert.Equal(t, "ut-entry", entry.GetDescription())
	assert.NotEmpty(t, entry.String())
	assert.Equal(t, "ut-user", entry.User)
	assert.Equal(t, "ut-pass", entry.pass)
	assert.Equal(t, "ut-protocol", entry.Protocol)
	assert.Equal(t, "ut-addr", entry.Addr)
	assert.Empty(t, entry.GormDbMap)
	assert.NotEmpty(t, entry.GormConfigMap)
	assert.Equal(t, EncodingJson, entry.loggerEncoding)
	assert.Equal(t, LoggerLevelInfo, entry.loggerLevel)
	assert.NotEmpty(t, entry.loggerOutputPath)
	assert.NotNil(t, entry.Logger)

	// remove entry
	rkentry.GlobalAppCtx.RemoveEntry(entry.GetName())
}

func TestMySqlEntry_IsHealthy(t *testing.T) {
	// test with dry run enabled
	entry := RegisterMySqlEntry(
		WithLoggerLevel(LoggerLevelInfo),
		WithLoggerEncoding(EncodingConsole))
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

func TestToAbsPath(t *testing.T) {
	res := toAbsPath("ut-path")
	assert.Len(t, res, 1)

	assert.True(t, strings.HasPrefix(res[0], "/"))
}

func TestGetMySqlEntry(t *testing.T) {
	// expect nil
	assert.Nil(t, GetMySqlEntry("not-exist"))

	// with invalid entry
	assert.Nil(t, GetMySqlEntry(rkentry.GlobalAppCtx.GetAppInfoEntry().GetName()))

	entry := RegisterMySqlEntry()
	defer rkentry.GlobalAppCtx.RemoveEntry(entry.GetName())
	// happy case
	assert.Equal(t, entry, GetMySqlEntry(entry.GetName()))
}

func TestRegisterMySqlEntriesWithConfig(t *testing.T) {
	bootConfigStr := `
mySql:
  - name: user-db
    enabled: true
    locale: "*::*::*::*"
    addr: "localhost:3306"
    user: root
    pass: pass
    logger:
      level: warn
      encoding: json
      outputPaths: [ "mysql/log" ]
    database:
      - name: user
        autoCreate: true
        dryRun: false
        params:
          - "charset=utf8mb4"
          - "parseTime=True"
          - "loc=Local"
`

	tempDir := path.Join(t.TempDir(), "boot.yaml")
	assert.Nil(t, ioutil.WriteFile(tempDir, []byte(bootConfigStr), os.ModePerm))

	entries := RegisterMySqlEntriesWithConfig(tempDir)

	assert.NotEmpty(t, entries)

	rkentry.GlobalAppCtx.RemoveEntry("user-db")
}

func TestMySqlEntry_Bootstrap(t *testing.T) {
	defer assertNotPanic(t)

	entry := RegisterMySqlEntry(
		WithDatabase("ut-database", false, true))
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
