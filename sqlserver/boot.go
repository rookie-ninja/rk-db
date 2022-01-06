// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.

// Package rksqlserver is an implementation of rkentry.Entry which could be used gorm.DB instance.
package rksqlserver

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rookie-ninja/rk-common/common"
	"github.com/rookie-ninja/rk-entry/entry"
	"github.com/rookie-ninja/rk-logger"
	"go.uber.org/zap"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"path"
	"strings"
	"time"
)

const (
	// EncodingConsole console encoding style of logging
	EncodingConsole = "console"
	// EncodingJson console encoding style of logging
	EncodingJson = "json"

	// LoggerLevelSilent set logger level of gorm as silent
	LoggerLevelSilent = "silent"
	// LoggerLevelError set logger level of gorm as error
	LoggerLevelError = "error"
	// LoggerLevelWarn set logger level of gorm as warn
	LoggerLevelWarn = "warn"
	// LoggerLevelInfo set logger level of gorm as info
	LoggerLevelInfo = "info"

	createDbSql = `
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '%s')
BEGIN
  CREATE DATABASE [%s];
END;
`
)

// This must be declared in order to register registration function into rk context
// otherwise, rk-boot won't able to bootstrap echo entry automatically from boot config file
func init() {
	rkentry.RegisterEntryRegFunc(RegisterSqlServerEntriesWithConfig)
}

// BootConfig
// SqlServer entry boot config which reflects to YAML config
type BootConfig struct {
	SqlServer []struct {
		Enabled     bool   `yaml:"enabled" json:"enabled"`
		Name        string `yaml:"name" json:"name"`
		Description string `yaml:"description" json:"description"`
		Locale      string `yaml:"locale" json:"locale"`
		User        string `yaml:"user" json:"user"`
		Pass        string `yaml:"pass" json:"pass"`
		Addr        string `yaml:"addr" json:"addr"`
		Database    []struct {
			Name       string   `yaml:"name" json:"name"`
			Params     []string `yaml:"params" json:"params"`
			DryRun     bool     `yaml:"dryRun" json:"dryRun"`
			AutoCreate bool     `yaml:"autoCreate" json:"autoCreate"`
		} `yaml:"database" json:"database"`
		Logger struct {
			Encoding    string   `yaml:"encoding" json:"encoding"`
			Level       string   `yaml:"level" json:"level"`
			OutputPaths []string `yaml:"outputPaths" json:"outputPaths"`
		}
	} `yaml:"sqlServer" json:"sqlServer"`
}

// SqlServerEntry will init gorm.DB or SqlMock with provided arguments
type SqlServerEntry struct {
	EntryName        string                  `yaml:"entryName" yaml:"entryName"`
	EntryType        string                  `yaml:"entryType" yaml:"entryType"`
	EntryDescription string                  `yaml:"-" json:"-"`
	User             string                  `yaml:"user" json:"user"`
	pass             string                  `yaml:"-" json:"-"`
	loggerEncoding   string                  `yaml:"-" json:"-"`
	loggerOutputPath []string                `yaml:"-" json:"-"`
	loggerLevel      string                  `yaml:"-" json:"-"`
	Logger           *zap.Logger             `yaml:"-" json:"-"`
	Addr             string                  `yaml:"addr" json:"addr"`
	innerDbList      []*databaseInner        `yaml:"-" json:"-"`
	GormDbMap        map[string]*gorm.DB     `yaml:"-" json:"-"`
	GormConfigMap    map[string]*gorm.Config `yaml:"-" json:"-"`
}

type databaseInner struct {
	name       string
	dryRun     bool
	autoCreate bool
	params     []string
}

// DataStore will be extended in future.
type Option func(*SqlServerEntry)

// WithName provide name.
func WithName(name string) Option {
	return func(entry *SqlServerEntry) {
		entry.EntryName = name
	}
}

// WithDescription provide name.
func WithDescription(description string) Option {
	return func(entry *SqlServerEntry) {
		entry.EntryDescription = description
	}
}

// WithUser provide user
func WithUser(user string) Option {
	return func(m *SqlServerEntry) {
		if len(user) > 0 {
			m.User = user
		}
	}
}

// WithPass provide password
func WithPass(pass string) Option {
	return func(m *SqlServerEntry) {
		if len(pass) > 0 {
			m.pass = pass
		}
	}
}

// WithAddr provide address
func WithAddr(addr string) Option {
	return func(m *SqlServerEntry) {
		if len(addr) > 0 {
			m.Addr = addr
		}
	}
}

// WithDatabase provide database
func WithDatabase(name string, dryRun, autoCreate bool, params ...string) Option {
	return func(m *SqlServerEntry) {
		if len(name) < 1 {
			return
		}

		innerDb := &databaseInner{
			name:       name,
			dryRun:     dryRun,
			autoCreate: autoCreate,
			params:     make([]string, 0),
		}

		innerDb.params = append(innerDb.params, params...)

		m.innerDbList = append(m.innerDbList, innerDb)
	}
}

// WithLoggerEncoding provide console=0, json=1.
// json or console is supported.
func WithLoggerEncoding(ec string) Option {
	return func(m *SqlServerEntry) {
		m.loggerEncoding = strings.ToLower(ec)
	}
}

// WithLoggerOutputPaths provide Logger Output Path.
// Multiple output path could be supported including stdout.
func WithLoggerOutputPaths(path ...string) Option {
	return func(m *SqlServerEntry) {
		m.loggerOutputPath = append(m.loggerOutputPath, path...)
	}
}

// WithLoggerLevel provide logger level in gorm
// Available options are silent, error, warn, info which matches gorm.logger
func WithLoggerLevel(level string) Option {
	return func(m *SqlServerEntry) {
		m.loggerLevel = strings.ToLower(level)
	}
}

// RegisterSqlServerEntriesWithConfig register SqlServerEntry based on config file into rkentry.GlobalAppCtx
func RegisterSqlServerEntriesWithConfig(configFilePath string) map[string]rkentry.Entry {
	res := make(map[string]rkentry.Entry)

	// 1: unmarshal user provided config into boot config struct
	config := &BootConfig{}
	rkcommon.UnmarshalBootConfig(configFilePath, config)

	for _, element := range config.SqlServer {
		if len(element.Name) < 1 || !rkcommon.MatchLocaleWithEnv(element.Locale) {
			continue
		}

		opts := []Option{
			WithName(element.Name),
			WithDescription(element.Description),
			WithUser(element.User),
			WithPass(element.Pass),
			WithAddr(element.Addr),
			WithLoggerEncoding(element.Logger.Encoding),
			WithLoggerOutputPaths(element.Logger.OutputPaths...),
			WithLoggerLevel(element.Logger.Level),
		}

		// iterate database section
		for _, db := range element.Database {
			opts = append(opts, WithDatabase(db.Name, db.DryRun, db.AutoCreate, db.Params...))
		}

		entry := RegisterSqlServerEntry(opts...)

		res[element.Name] = entry
	}

	return res
}

// RegisterSqlServerEntry will register Entry into GlobalAppCtx
func RegisterSqlServerEntry(opts ...Option) *SqlServerEntry {
	entry := &SqlServerEntry{
		EntryName:        "SqlServer",
		EntryType:        "SqlServer",
		EntryDescription: "SqlServer entry for gorm.DB",
		User:             "sa",
		pass:             "pass",
		Addr:             "localhost:1433",
		innerDbList:      make([]*databaseInner, 0),
		loggerOutputPath: make([]string, 0),
		loggerEncoding:   EncodingConsole,
		loggerLevel:      LoggerLevelWarn,
		GormDbMap:        make(map[string]*gorm.DB),
		GormConfigMap:    make(map[string]*gorm.Config),
	}

	for i := range opts {
		opts[i](entry)
	}

	if len(entry.EntryDescription) < 1 {
		entry.EntryDescription = fmt.Sprintf("%s entry with name of %s, addr:%s, user:%s",
			entry.EntryType,
			entry.EntryName,
			entry.Addr,
			entry.User)
	}

	// Override zap logger encoding and output path if provided by user
	// Override encoding type
	zapLoggerConfig := rklogger.NewZapStdoutConfig()
	lumberjackConfig := rklogger.NewLumberjackConfigDefault()
	if entry.loggerEncoding == EncodingJson || len(entry.loggerOutputPath) > 0 {
		if entry.loggerEncoding == EncodingJson {
			zapLoggerConfig.Encoding = "json"
		}

		if len(entry.loggerOutputPath) > 0 {
			zapLoggerConfig.OutputPaths = toAbsPath(entry.loggerOutputPath...)
		}

		fmt.Println(zapLoggerConfig.OutputPaths)

		if lumberjackConfig == nil {
			lumberjackConfig = rklogger.NewLumberjackConfigDefault()
		}
	}

	if logger, err := rklogger.NewZapLoggerWithConf(zapLoggerConfig, lumberjackConfig); err != nil {
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

// Bootstrap SqlServerEntry
func (entry *SqlServerEntry) Bootstrap(ctx context.Context) {
	entry.Logger.Info("Bootstrap sqlServer entry",
		zap.String("entryName", entry.EntryName),
		zap.String("sqlServerUser", entry.User),
		zap.String("sqlServerAddr", entry.Addr))

	// Connect and create db if missing
	if err := entry.connect(); err != nil {
		entry.Logger.Error("failed to connect to database", zap.Error(err))
		rkcommon.ShutdownWithError(fmt.Errorf("failed to connect to database at %s:%s@%s",
			entry.User, "****", entry.Addr))
	}
}

// Interrupt SqlServerEntry
func (entry *SqlServerEntry) Interrupt(ctx context.Context) {
	entry.Logger.Info("Interrupt sqlServer entry",
		zap.String("entryName", entry.EntryName),
		zap.String("sqlServerUser", entry.User),
		zap.String("sqlServerAddr", entry.Addr))
}

// GetName returns entry name
func (entry *SqlServerEntry) GetName() string {
	return entry.EntryName
}

// GetType returns entry type
func (entry *SqlServerEntry) GetType() string {
	return entry.EntryType
}

// GetDescription returns entry description
func (entry *SqlServerEntry) GetDescription() string {
	return entry.EntryDescription
}

// String returns json marshalled string
func (entry *SqlServerEntry) String() string {
	bytes, err := json.Marshal(entry)
	if err != nil || len(bytes) < 1 {
		return "{}"
	}

	return string(bytes)
}

// IsHealthy checks healthy status remote provider
func (entry *SqlServerEntry) IsHealthy() bool {
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

func (entry *SqlServerEntry) GetDB(name string) *gorm.DB {
	return entry.GormDbMap[name]
}

// Create database if missing
func (entry *SqlServerEntry) connect() error {
	for _, innerDb := range entry.innerDbList {
		var db *gorm.DB
		var err error

		// 1: create db if missing
		if !innerDb.dryRun && innerDb.autoCreate {
			dsn := fmt.Sprintf("sqlserver://%s:%s@%s",
				entry.User, entry.pass, entry.Addr)

			fmt.Println(dsn)

			db, err = gorm.Open(sqlserver.Open(dsn), entry.GormConfigMap[innerDb.name])

			// failed to connect to database
			if err != nil {
				return err
			}

			createSQL := fmt.Sprintf(createDbSql, innerDb.name, innerDb.name)
			fmt.Println(createSQL)

			db = db.Exec(createSQL)

			if db.Error != nil {
				return db.Error
			}
		}

		params := []string{fmt.Sprintf("database=%s", innerDb.name)}
		params = append(params, innerDb.params...)

		dsn := fmt.Sprintf("sqlserver://%s:%s@%s/?%s",
			entry.User, entry.pass, entry.Addr, strings.Join(params, "&"))

		db, err = gorm.Open(sqlserver.Open(dsn), entry.GormConfigMap[innerDb.name])

		// failed to connect to database
		if err != nil {
			return err
		}

		entry.GormDbMap[innerDb.name] = db
	}

	return nil
}

// Make incoming paths to absolute path with current working directory attached as prefix
func toAbsPath(p ...string) []string {
	res := make([]string, 0)

	for i := range p {
		newPath := p[i]
		if !path.IsAbs(p[i]) {
			wd, _ := os.Getwd()
			newPath = path.Join(wd, p[i])
		}
		res = append(res, newPath)
	}

	return res
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

// GetSqlServerEntry returns SqlServerEntry instance
func GetSqlServerEntry(name string) *SqlServerEntry {
	if raw := rkentry.GlobalAppCtx.GetEntry(name); raw != nil {
		if entry, ok := raw.(*SqlServerEntry); ok {
			return entry
		}
	}

	return nil
}
