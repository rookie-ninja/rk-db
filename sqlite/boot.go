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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rookie-ninja/rk-db/sqlite/plugins"
	"github.com/rookie-ninja/rk-entry/v2/entry"
	"github.com/rookie-ninja/rk-logger"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// This must be declared in order to register registration function into rk context
// otherwise, rk-boot won't able to bootstrap echo entry automatically from boot config file
func init() {
	rkentry.RegisterPluginRegFunc(RegisterSqliteEntryYAML)
}

const SqliteEntryType = "SqliteEntry"

// BootSqlite
// SqliteEntry entry boot config which reflects to YAML config
type BootSqlite struct {
	Sqlite []*BootSqliteE `yaml:"sqlite" json:"sqlite"`
}

type BootSqliteE struct {
	Enabled     bool   `yaml:"enabled" json:"enabled"`
	Name        string `yaml:"name" json:"name"`
	Description string `yaml:"description" json:"description"`
	Domain      string `yaml:"domain" json:"domain"`
	Database    []struct {
		Name     string   `yaml:"name" json:"name"`
		DbDir    string   `yaml:"dbDir" json:"dbDir"`
		InMemory bool     `yaml:"inMemory" json:"inMemory"`
		Params   []string `yaml:"params" json:"params"`
		DryRun   bool     `yaml:"dryRun" json:"dryRun"`
		Plugins  struct {
			Prom plugins.PromConfig `yaml:"prom"`
		} `yaml:"plugins" json:"plugins"`
	} `yaml:"database" json:"database"`
	Logger struct {
		Entry                     string   `json:"entry" yaml:"entry"`
		Level                     string   `json:"level" yaml:"level"`
		Encoding                  string   `json:"encoding" yaml:"encoding"`
		OutputPaths               []string `json:"outputPaths" yaml:"outputPaths"`
		SlowThresholdMs           int      `json:"slowThresholdMs" yaml:"slowThresholdMs"`
		IgnoreRecordNotFoundError bool     `json:"ignoreRecordNotFoundError" yaml:"ignoreRecordNotFoundError"`
	} `json:"logger" yaml:"logger"`
}

// SqliteEntry will init gorm.DB or SqlMock with provided arguments
type SqliteEntry struct {
	entryName        string                  `yaml:"entryName" yaml:"entryName"`
	entryType        string                  `yaml:"entryType" yaml:"entryType"`
	entryDescription string                  `yaml:"-" json:"-"`
	logger           *Logger                 `yaml:"-" json:"-"`
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
	plugins  []gorm.Plugin
}

// Option will be extended in the future.
type Option func(*SqliteEntry)

// WithName provide name.
func WithName(name string) Option {
	return func(entry *SqliteEntry) {
		entry.entryName = name
	}
}

// WithDescription provide name.
func WithDescription(description string) Option {
	return func(entry *SqliteEntry) {
		entry.entryDescription = description
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

func WithPlugin(name string, plugin gorm.Plugin) Option {
	return func(entry *SqliteEntry) {
		if name == "" || plugin == nil {
			return
		}
		for i := range entry.innerDbList {
			inner := entry.innerDbList[i]
			if inner.name == name {
				inner.plugins = append(inner.plugins, plugin)
			}
		}
	}
}

// WithLogger provide Logger
func WithLogger(logger *Logger) Option {
	return func(m *SqliteEntry) {
		if logger != nil {
			m.logger = logger
		}
	}
}

// RegisterSqliteEntryYAML register SqliteEntry based on config file into rkentry.GlobalAppCtx
func RegisterSqliteEntryYAML(raw []byte) map[string]rkentry.Entry {
	res := make(map[string]rkentry.Entry)

	// 1: unmarshal user provided config into boot config struct
	config := &BootSqlite{}
	rkentry.UnmarshalBootYAML(raw, config)

	// filter out based domain
	configMap := make(map[string]*BootSqliteE)
	for _, e := range config.Sqlite {
		if !e.Enabled || len(e.Name) < 1 {
			continue
		}

		if !rkentry.IsValidDomain(e.Domain) {
			continue
		}

		// * or matching domain
		// 1: add it to map if missing
		if _, ok := configMap[e.Name]; !ok {
			configMap[e.Name] = e
			continue
		}

		// 2: already has an entry, then compare domain,
		//    only one case would occur, previous one is already the correct one, continue
		if e.Domain == "" || e.Domain == "*" {
			continue
		}

		configMap[e.Name] = e
	}

	for _, element := range configMap {
		logger := &Logger{
			LogLevel:                  gormLogger.Warn,
			SlowThreshold:             5000 * time.Millisecond,
			IgnoreRecordNotFoundError: element.Logger.IgnoreRecordNotFoundError,
		}

		// configure log level
		switch element.Logger.Level {
		case "info":
			logger.LogLevel = gormLogger.Info
		case "warn":
			logger.LogLevel = gormLogger.Warn
		case "error":
			logger.LogLevel = gormLogger.Error
		case "silent":
			logger.LogLevel = gormLogger.Silent
		}

		// configure slow threshold
		if element.Logger.SlowThresholdMs > 0 {
			logger.SlowThreshold = time.Duration(element.Logger.SlowThresholdMs) * time.Millisecond
		}

		// assign logger entry
		loggerEntry := rkentry.GlobalAppCtx.GetLoggerEntry(element.Logger.Entry)
		if loggerEntry == nil {
			loggerEntry = rkentry.GlobalAppCtx.GetLoggerEntryDefault()
		}

		// Override zap logger encoding and output path if provided by user
		// Override encoding type
		if element.Logger.Encoding == "json" || len(element.Logger.OutputPaths) > 0 {
			if element.Logger.Encoding == "json" {
				loggerEntry.LoggerConfig.Encoding = "json"
			}

			if len(element.Logger.OutputPaths) > 0 {
				loggerEntry.LoggerConfig.OutputPaths = toAbsPath(element.Logger.OutputPaths...)
			}

			if loggerEntry.LumberjackConfig == nil {
				loggerEntry.LumberjackConfig = rklogger.NewLumberjackConfigDefault()
			}

			if newLogger, err := rklogger.NewZapLoggerWithConf(loggerEntry.LoggerConfig, loggerEntry.LumberjackConfig); err != nil {
				rkentry.ShutdownWithError(err)
			} else {
				logger.delegate = newLogger.WithOptions(zap.WithCaller(true))
			}
		} else {
			logger.delegate = loggerEntry.Logger.WithOptions(zap.WithCaller(true))
		}

		opts := []Option{
			WithName(element.Name),
			WithDescription(element.Description),
			WithLogger(logger),
		}

		// iterate database section
		for _, db := range element.Database {
			opts = append(opts, WithDatabase(db.Name, db.DbDir, db.DryRun, db.InMemory, db.Params...))

			if db.Plugins.Prom.Enabled {
				if db.InMemory {
					db.Plugins.Prom.DbAddr = "inMemory"
				} else {
					db.Plugins.Prom.DbAddr = db.DbDir
				}
				db.Plugins.Prom.DbName = db.Name
				db.Plugins.Prom.DbType = "sqlite"
				prom := plugins.NewProm(&db.Plugins.Prom)
				opts = append(opts, WithPlugin(db.Name, prom))
			}
		}

		entry := RegisterSqliteEntry(opts...)

		res[element.Name] = entry
	}

	return res
}

// RegisterSqliteEntry will register Entry into GlobalAppCtx
func RegisterSqliteEntry(opts ...Option) *SqliteEntry {
	entry := &SqliteEntry{
		entryName:        "Sqlite",
		entryType:        SqliteEntryType,
		entryDescription: "Sqlite entry for gorm.DB",
		innerDbList:      make([]*databaseInner, 0),
		GormDbMap:        make(map[string]*gorm.DB),
		GormConfigMap:    make(map[string]*gorm.Config),
	}

	entry.logger = &Logger{
		delegate:                  rkentry.GlobalAppCtx.GetLoggerEntryDefault().Logger,
		SlowThreshold:             5000 * time.Millisecond,
		LogLevel:                  gormLogger.Warn,
		IgnoreRecordNotFoundError: false,
	}

	for i := range opts {
		opts[i](entry)
	}

	// create default gorm configs for databases
	for _, innerDb := range entry.innerDbList {
		entry.GormConfigMap[innerDb.name] = &gorm.Config{
			Logger: entry.logger,
			DryRun: innerDb.dryRun,
		}
	}

	rkentry.GlobalAppCtx.AddEntry(entry)

	return entry
}

// Bootstrap SqliteEntry
func (entry *SqliteEntry) Bootstrap(ctx context.Context) {
	// extract eventId if exists
	fields := make([]zap.Field, 0)

	if val := ctx.Value("eventId"); val != nil {
		if id, ok := val.(string); ok {
			fields = append(fields, zap.String("eventId", id))
		}
	}

	fields = append(fields,
		zap.String("entryName", entry.entryName),
		zap.String("entryType", entry.entryType))

	entry.logger.delegate.Info("Bootstrap SQLiteEntry", fields...)

	// Connect and create db if missing
	if err := entry.connect(); err != nil {
		fields = append(fields, zap.Error(err))
		entry.logger.delegate.Error("Failed to connect to database", fields...)
		rkentry.ShutdownWithError(errors.New("failed to connect to database"))
	}
}

// Interrupt SqliteEntry
func (entry *SqliteEntry) Interrupt(ctx context.Context) {
	for _, db := range entry.GormDbMap {
		closeDB(db)
	}

	// extract eventId if exists
	fields := make([]zap.Field, 0)

	if val := ctx.Value("eventId"); val != nil {
		if id, ok := val.(string); ok {
			fields = append(fields, zap.String("eventId", id))
		}
	}

	fields = append(fields,
		zap.String("entryName", entry.entryName),
		zap.String("entryType", entry.entryType))

	entry.logger.delegate.Info("Interrupt SQLiteEntry", fields...)
}

// GetName returns entry name
func (entry *SqliteEntry) GetName() string {
	return entry.entryName
}

// GetType returns entry type
func (entry *SqliteEntry) GetType() string {
	return entry.entryType
}

// GetDescription returns entry description
func (entry *SqliteEntry) GetDescription() string {
	return entry.entryDescription
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

func (entry *SqliteEntry) RegisterPromMetrics(registry *prometheus.Registry) error {
	for i := range entry.innerDbList {
		innerDb := entry.innerDbList[i]
		for j := range innerDb.plugins {
			p := innerDb.plugins[j]
			if v, ok := p.(*plugins.Prom); ok {
				gaugeList := v.MetricsSet.ListGauges()
				for k := range gaugeList {
					if err := registry.Register(gaugeList[k]); err != nil {
						return err
					}
				}
				counterList := v.MetricsSet.ListCounters()
				for k := range counterList {
					if err := registry.Register(counterList[k]); err != nil {
						return err
					}
				}
				summaryList := v.MetricsSet.ListSummaries()
				for k := range summaryList {
					if err := registry.Register(summaryList[k]); err != nil {
						return err
					}
				}
				hisList := v.MetricsSet.ListHistograms()
				for k := range hisList {
					if err := registry.Register(hisList[k]); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
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

		entry.logger.delegate.Info(fmt.Sprintf("Connecting to database [%s]", innerDb.name))

		// 1: create directory if missing
		if !filepath.IsAbs(filepath.ToSlash(innerDb.dbDir)) {
			wd, err := os.Getwd()
			if err != nil {
				return err
			}

			innerDb.dbDir = filepath.ToSlash(filepath.Join(wd, innerDb.dbDir))
			err = os.MkdirAll(innerDb.dbDir, os.ModePerm)
			if err != nil {
				return err
			}
		}

		dbFile = filepath.ToSlash(filepath.Join(innerDb.dbDir, innerDb.name+".db"))

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

		for i := range innerDb.plugins {
			plugin := innerDb.plugins[i]
			if promPlugin, ok := plugin.(*plugins.Prom); ok {
				if innerDb.inMemory {
					promPlugin.Conf.DbAddr = "memory"
				} else {
					promPlugin.Conf.DbAddr = dbFile
				}
			}
			if err := db.Use(innerDb.plugins[i]); err != nil {
				return err
			}
		}

		entry.GormDbMap[innerDb.name] = db
		entry.logger.delegate.Info(fmt.Sprintf("Connecting to database [%s] success", innerDb.name))
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
	if raw := rkentry.GlobalAppCtx.GetEntry(SqliteEntryType, name); raw != nil {
		if entry, ok := raw.(*SqliteEntry); ok {
			return entry
		}
	}

	return nil
}

// Make incoming paths to absolute path with current working directory attached as prefix
func toAbsPath(p ...string) []string {
	res := make([]string, 0)

	for i := range p {
		if filepath.IsAbs(filepath.ToSlash(p[i])) || p[i] == "stdout" || p[i] == "stderr" {
			res = append(res, p[i])
			continue
		}
		wd, _ := os.Getwd()
		res = append(res, filepath.ToSlash(filepath.Join(wd, p[i])))
	}

	return res
}

func closeDB(db *gorm.DB) {
	if db != nil {
		inner, _ := db.DB()
		if inner != nil {
			inner.Close()
		}
	}
}
