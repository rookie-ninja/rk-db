// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.
package rksqlite

import (
	"context"
	"github.com/rookie-ninja/rk-entry/entry"
	rklogger "github.com/rookie-ninja/rk-logger"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func TestRegisterSqliteEntry(t *testing.T) {
	// without options
	entry := RegisterSqliteEntry()

	assert.NotEmpty(t, entry.GetName())
	assert.NotEmpty(t, entry.GetType())
	assert.NotEmpty(t, entry.GetDescription())
	assert.NotEmpty(t, entry.String())
	assert.Empty(t, entry.GormDbMap)
	assert.Empty(t, entry.GormConfigMap)
	assert.Equal(t, rklogger.EncodingConsole, entry.loggerEncoding)
	assert.Equal(t, LoggerLevelWarn, entry.loggerLevel)
	assert.Empty(t, entry.loggerOutputPath)
	assert.NotNil(t, entry.Logger)

	// remove entry
	rkentry.GlobalAppCtx.RemoveEntry(entry.GetName())

	// with options
	entry = RegisterSqliteEntry(
		WithName("ut-entry"),
		WithDescription("ut-entry"),
		WithDatabase("ut-database", "", true, true),
		WithLoggerEncoding(rklogger.EncodingJson),
		WithLoggerOutputPaths("ut-output"),
		WithLoggerLevel(LoggerLevelInfo))

	assert.Equal(t, "ut-entry", entry.GetName())
	assert.NotEmpty(t, entry.GetType())
	assert.Equal(t, "ut-entry", entry.GetDescription())
	assert.NotEmpty(t, entry.String())
	assert.Empty(t, entry.GormDbMap)
	assert.NotEmpty(t, entry.GormConfigMap)
	assert.Equal(t, rklogger.EncodingJson, entry.loggerEncoding)
	assert.Equal(t, LoggerLevelInfo, entry.loggerLevel)
	assert.NotEmpty(t, entry.loggerOutputPath)
	assert.NotNil(t, entry.Logger)

	// remove entry
	rkentry.GlobalAppCtx.RemoveEntry(entry.GetName())
}

func TestSqliteEntry_IsHealthy(t *testing.T) {
	// test with dry run enabled
	entry := RegisterSqliteEntry(
		WithLoggerLevel(LoggerLevelInfo),
		WithLoggerEncoding(rklogger.EncodingConsole))
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

func TestGetSqliteEntry(t *testing.T) {
	// expect nil
	assert.Nil(t, GetSqliteEntry("not-exist"))

	// with invalid entry
	assert.Nil(t, GetSqliteEntry(rkentry.GlobalAppCtx.GetAppInfoEntry().GetName()))

	entry := RegisterSqliteEntry()
	defer rkentry.GlobalAppCtx.RemoveEntry(entry.GetName())
	// happy case
	assert.Equal(t, entry, GetSqliteEntry(entry.GetName()))
}

func TestRegisterSqliteEntriesWithConfig(t *testing.T) {
	bootConfigStr := `
sqlite:
  - name: user-db                     # Required
    enabled: true                     # Required
    locale: "*::*::*::*"              # Required
    database:
      - name: user                    # Required
#        inMemory: true               # Optional, default: false
#        dbDir: ""                    # Optional, default: "", directory where db file created or imported, can be absolute or relative path
#        dryRun: true                 # Optional, default: false
#        params: []                   # Optional, default: ["cache=shared"]
#    logger:
#      level: warn                    # Optional, default: warn
#      encoding: json                 # Optional, default: console
#      outputPaths: [ "sqlite/log" ]  # Optional, default: []
`

	tempDir := path.Join(t.TempDir(), "boot.yaml")
	assert.Nil(t, ioutil.WriteFile(tempDir, []byte(bootConfigStr), os.ModePerm))

	entries := RegisterSqliteEntriesWithConfig(tempDir)

	assert.NotEmpty(t, entries)

	rkentry.GlobalAppCtx.RemoveEntry("user-db")
}

func TestSqliteEntry_Bootstrap(t *testing.T) {
	defer assertNotPanic(t)

	entry := RegisterSqliteEntry(
		WithDatabase("ut-database", "", false, true))
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
