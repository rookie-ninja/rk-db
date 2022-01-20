// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.

// Package rksqlite is an implementation of rkentry.Entry which could be used gorm.DB instance.
package rksqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rookie-ninja/rk-common/common"
	"github.com/rookie-ninja/rk-entry/entry"
	"github.com/rookie-ninja/rk-logger"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"path"
	"strings"
	"time"
)

const (
	// LoggerLevelSilent set logger level of gorm as silent
	LoggerLevelSilent = "silent"
	// LoggerLevelError set logger level of gorm as error
	LoggerLevelError = "error"
	// LoggerLevelWarn set logger level of gorm as warn
	LoggerLevelWarn = "warn"
	// LoggerLevelInfo set logger level of gorm as info
	LoggerLevelInfo = "info"
)

// This must be declared in order to register registration function into rk context
// otherwise, rk-boot won't able to bootstrap echo entry automatically from boot config file
func init() {
	rkentry.RegisterEntryRegFunc(RegisterSqliteEntriesWithConfig)
}

// BootConfig
// SqliteEntry entry boot config which reflects to YAML config
type BootConfig struct {
	Sqlite []struct {
		Enabled     bool   `yaml:"enabled" json:"enabled"`
		Name        string `yaml:"name" json:"name"`
		Description string `yaml:"description" json:"description"`
		Locale      string `yaml:"locale" json:"locale"`
		Database    []struct {
			Name     string   `yaml:"name" json:"name"`
			DbDir    string   `yaml:"dbDir" json:"dbDir"`
			InMemory bool     `yaml:"inMemory" json:"inMemory"`
			Params   []string `yaml:"params" json:"params"`
			DryRun   bool     `yaml:"dryRun" json:"dryRun"`
		} `yaml:"database" json:"database"`
		Logger struct {
			Encoding    string   `yaml:"encoding" json:"encoding"`
			Level       string   `yaml:"level" json:"level"`
			OutputPaths []string `yaml:"outputPaths" json:"outputPaths"`
		}
	} `yaml:"sqlite" json:"sqlite"`
}

// SqliteEntry will init gorm.DB or SqlMock with provided arguments
type SqliteEntry struct {
	EntryName        string                  `yaml:"entryName" yaml:"entryName"`
	EntryType        string                  `yaml:"entryType" yaml:"entryType"`
	EntryDescription string                  `yaml:"-" json:"-"`
	loggerEncoding   string                  `yaml:"-" json:"-"`
	loggerOutputPath []string                `yaml:"-" json:"-"`
	loggerLevel      string                  `yaml:"-" json:"-"`
	Logger           *zap.Logger             `yaml:"-" json:"-"`
	innerDbList      []*databaseInner        `yaml:"-" json:"-"`
	GormDbMap        map[string]*gorm.DB     `yaml:"-" json:"-"`
	GormConfigMap    map[string]*gorm.Config `yaml:"-" json:"-"`
}

type databaseInner struct {
	name     string
	dbDir    string
	inMemory bool
	dryRun   bool
	params   []string
}

// DataStore will be extended in future.
type Option func(*SqliteEntry)

// WithName provide name.
func WithName(name string) Option {
	return func(entry *SqliteEntry) {
		entry.EntryName = name
	}
}

// WithDescription provide name.
func WithDescription(description string) Option {
	return func(entry *SqliteEntry) {
		entry.EntryDescription = description
	}
}

// WithDatabase provide database
func WithDatabase(name, dbDir string, dryRun, inMemory bool, params ...string) Option {
	return func(m *SqliteEntry) {
		if len(name) < 1 {
			return
		}

		innerDb := &databaseInner{
			name:     name,
			dbDir:    dbDir,
			inMemory: inMemory,
			dryRun:   dryRun,
			params:   make([]string, 0),
		}

		// add default params if no param provided
		if len(params) < 1 {
			innerDb.params = append(innerDb.params,
				"cache=shared")
		} else {
			innerDb.params = append(innerDb.params, params...)
		}

		m.innerDbList = append(m.innerDbList, innerDb)
	}
}

// WithLoggerEncoding provide console=0, json=1.
// json or console is supported.
func WithLoggerEncoding(ec string) Option {
	return func(m *SqliteEntry) {
		m.loggerEncoding = strings.ToLower(ec)
	}
}

// WithLoggerOutputPaths provide Logger Output Path.
// Multiple output path could be supported including stdout.
func WithLoggerOutputPaths(path ...string) Option {
	return func(m *SqliteEntry) {
		m.loggerOutputPath = append(m.loggerOutputPath, path...)
	}
}

// WithLoggerLevel provide logger level in gorm
// Available options are silent, error, warn, info which matches gorm.logger
func WithLoggerLevel(level string) Option {
	return func(m *SqliteEntry) {
		m.loggerLevel = strings.ToLower(level)
	}
}

// RegisterSqliteEntriesWithConfig register SqliteEntry based on config file into rkentry.GlobalAppCtx
func RegisterSqliteEntriesWithConfig(configFilePath string) map[string]rkentry.Entry {
	res := make(map[string]rkentry.Entry)

	// 1: unmarshal user provided config into boot config struct
	config := &BootConfig{}
	rkcommon.UnmarshalBootConfig(configFilePath, config)

	for _, element := range config.Sqlite {
		if len(element.Name) < 1 || !rkcommon.MatchLocaleWithEnv(element.Locale) {
			continue
		}

		opts := []Option{
			WithName(element.Name),
			WithDescription(element.Description),
			WithLoggerEncoding(element.Logger.Encoding),
			WithLoggerOutputPaths(element.Logger.OutputPaths...),
			WithLoggerLevel(element.Logger.Level),
		}

		// iterate database section
		for _, db := range element.Database {
			opts = append(opts, WithDatabase(db.Name, db.DbDir, db.DryRun, db.InMemory, db.Params...))
		}

		entry := RegisterSqliteEntry(opts...)

		res[element.Name] = entry
	}

	return res
}

// RegisterSqliteEntry will register Entry into GlobalAppCtx
func RegisterSqliteEntry(opts ...Option) *SqliteEntry {
	entry := &SqliteEntry{
		EntryName:        "Sqlite",
		EntryType:        "Sqlite",
		EntryDescription: "Sqlite entry for gorm.DB",
		innerDbList:      make([]*databaseInner, 0),
		loggerOutputPath: make([]string, 0),
		loggerEncoding:   rklogger.EncodingConsole,
		loggerLevel:      LoggerLevelWarn,
		GormDbMap:        make(map[string]*gorm.DB),
		GormConfigMap:    make(map[string]*gorm.Config),
	}

	for i := range opts {
		opts[i](entry)
	}

	if len(entry.EntryDescription) < 1 {
		entry.EntryDescription = fmt.Sprintf("%s entry with name of %s",
			entry.EntryType,
			entry.EntryName)
	}

	// Override zap logger encoding and output path if provided by user
	// Override encoding type
	if logger, err := rklogger.NewZapLoggerWithOverride(entry.loggerEncoding, entry.loggerOutputPath...); err != nil {
		rkcommon.ShutdownWithError(err)
	} else {
		entry.Logger = logger
	}

	// convert logger level to gorm type
	loggerLevel := logger.Warn
	switch entry.loggerLevel {
	case LoggerLevelSilent:
		loggerLevel = logger.Silent
	case LoggerLevelWarn:
		loggerLevel = logger.Warn
	case LoggerLevelError:
		loggerLevel = logger.Error
	case LoggerLevelInfo:
		loggerLevel = logger.Info
	default:
		loggerLevel = logger.Warn
	}

	// create default gorm configs for databases
	for _, innerDb := range entry.innerDbList {
		entry.GormConfigMap[innerDb.name] = &gorm.Config{
			Logger: logger.New(NewLogger(entry.Logger), logger.Config{
				SlowThreshold:             200 * time.Millisecond,
				LogLevel:                  loggerLevel,
				IgnoreRecordNotFoundError: false,
				Colorful:                  false,
			}),
			DryRun: innerDb.dryRun,
		}
	}

	rkentry.GlobalAppCtx.AddEntry(entry)

	return entry
}

// Bootstrap SqliteEntry
func (entry *SqliteEntry) Bootstrap(ctx context.Context) {
	entry.Logger.Info("Bootstrap SQLite entry",
		zap.String("entryName", entry.EntryName))

	// Connect and create db if missing
	if err := entry.connect(); err != nil {
		entry.Logger.Error("Failed to connect to database", zap.Error(err))
		rkcommon.ShutdownWithError(errors.New("failed to connect to database"))
	}
}

// Interrupt SqliteEntry
func (entry *SqliteEntry) Interrupt(ctx context.Context) {
	entry.Logger.Info("Interrupt SQLite entry",
		zap.String("entryName", entry.EntryName))
}

// GetName returns entry name
func (entry *SqliteEntry) GetName() string {
	return entry.EntryName
}

// GetType returns entry type
func (entry *SqliteEntry) GetType() string {
	return entry.EntryType
}

// GetDescription returns entry description
func (entry *SqliteEntry) GetDescription() string {
	return entry.EntryDescription
}

// String returns json marshalled string
func (entry *SqliteEntry) String() string {
	bytes, err := json.Marshal(entry)
	if err != nil || len(bytes) < 1 {
		return "{}"
	}

	return string(bytes)
}

// IsHealthy checks healthy status remote provider
func (entry *SqliteEntry) IsHealthy() bool {
	for _, gormDb := range entry.GormDbMap {
		if db, err := gormDb.DB(); err != nil {
			return false
		} else {
			if err := db.Ping(); err != nil {
				return false
			}
		}
	}

	return true
}

func (entry *SqliteEntry) GetDB(name string) *gorm.DB {
	return entry.GormDbMap[name]
}

// Create database if missing
func (entry *SqliteEntry) connect() error {
	for _, innerDb := range entry.innerDbList {
		var db *gorm.DB
		var err error
		var dbFile string

		entry.Logger.Info(fmt.Sprintf("Connecting to database [%s]", innerDb.name))

		// 1: create directory if missing
		if !path.IsAbs(innerDb.dbDir) {
			wd, err := os.Getwd()
			if err != nil {
				return err
			}

			innerDb.dbDir = path.Join(wd, innerDb.dbDir)
			err = os.MkdirAll(innerDb.dbDir, os.ModePerm)
			if err != nil {
				return err
			}
		}

		dbFile = path.Join(innerDb.dbDir, innerDb.name+".db")

		// 2: create dsn
		params := []string{fmt.Sprintf("file:%s?", dbFile)}
		params = append(params, innerDb.params...)

		// 3: is memory mode?
		if innerDb.inMemory {
			params = append(params, "mode=memory")
		}

		dsn := strings.Join(params, "&")

		db, err = gorm.Open(sqlite.Open(dsn), entry.GormConfigMap[innerDb.name])

		// failed to connect to database
		if err != nil {
			return err
		}

		entry.GormDbMap[innerDb.name] = db
		entry.Logger.Info(fmt.Sprintf("Connecting to database [%s] success", innerDb.name))
	}

	return nil
}

// Copy zap.Config
func copyZapLoggerConfig(src *zap.Config) *zap.Config {
	res := &zap.Config{
		Level:             src.Level,
		Development:       src.Development,
		DisableCaller:     src.DisableCaller,
		DisableStacktrace: src.DisableStacktrace,
		Sampling:          src.Sampling,
		Encoding:          src.Encoding,
		EncoderConfig:     src.EncoderConfig,
		OutputPaths:       src.OutputPaths,
		ErrorOutputPaths:  src.ErrorOutputPaths,
		InitialFields:     src.InitialFields,
	}

	return res
}

// GetSqliteEntry returns SqliteEntry instance
func GetSqliteEntry(name string) *SqliteEntry {
	if raw := rkentry.GlobalAppCtx.GetEntry(name); raw != nil {
		if entry, ok := raw.(*SqliteEntry); ok {
			return entry
		}
	}

	return nil
}
