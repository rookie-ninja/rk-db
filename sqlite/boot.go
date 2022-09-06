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
	"github.com/rookie-ninja/rk-entry/v2/entry"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"path"
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
	} `yaml:"database" json:"database"`
	LoggerEntry string `yaml:"loggerEntry" json:"loggerEntry"`
}

// SqliteEntry will init gorm.DB or SqlMock with provided arguments
type SqliteEntry struct {
	entryName        string                  `yaml:"entryName" yaml:"entryName"`
	entryType        string                  `yaml:"entryType" yaml:"entryType"`
	entryDescription string                  `yaml:"-" json:"-"`
	loggerEntry      *rkentry.LoggerEntry    `yaml:"-" json:"-"`
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

// Option will be extended in future.
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

// WithLoggerEntry provide rkentry.LoggerEntry entry name
func WithLoggerEntry(entry *rkentry.LoggerEntry) Option {
	return func(m *SqliteEntry) {
		if entry != nil {
			m.loggerEntry = entry
		} else {
			m.loggerEntry = rkentry.GlobalAppCtx.GetLoggerEntryDefault()
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
		opts := []Option{
			WithName(element.Name),
			WithDescription(element.Description),
			WithLoggerEntry(rkentry.GlobalAppCtx.GetLoggerEntry(element.LoggerEntry)),
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
		entryName:        "Sqlite",
		entryType:        SqliteEntryType,
		entryDescription: "Sqlite entry for gorm.DB",
		innerDbList:      make([]*databaseInner, 0),
		loggerEntry:      rkentry.GlobalAppCtx.GetLoggerEntryDefault(),
		GormDbMap:        make(map[string]*gorm.DB),
		GormConfigMap:    make(map[string]*gorm.Config),
	}

	for i := range opts {
		opts[i](entry)
	}

	// create default gorm configs for databases
	for _, innerDb := range entry.innerDbList {
		entry.GormConfigMap[innerDb.name] = &gorm.Config{
			Logger: &Logger{
				delegate:                  entry.loggerEntry.Logger,
				SlowThreshold:             5000 * time.Millisecond,
				LogLevel:                  logger.Warn,
				IgnoreRecordNotFoundError: false,
			},
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

	entry.loggerEntry.Info("Bootstrap SQLiteEntry", fields...)

	// Connect and create db if missing
	if err := entry.connect(); err != nil {
		fields = append(fields, zap.Error(err))
		entry.loggerEntry.Error("Failed to connect to database", fields...)
		rkentry.ShutdownWithError(errors.New("failed to connect to database"))
	}
}

// Interrupt SqliteEntry
func (entry *SqliteEntry) Interrupt(ctx context.Context) {
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

	entry.loggerEntry.Info("Interrupt SQLiteEntry", fields...)
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

func (entry *SqliteEntry) GetDB(name string) *gorm.DB {
	return entry.GormDbMap[name]
}

// Create database if missing
func (entry *SqliteEntry) connect() error {
	for _, innerDb := range entry.innerDbList {
		var db *gorm.DB
		var err error
		var dbFile string

		entry.loggerEntry.Info(fmt.Sprintf("Connecting to database [%s]", innerDb.name))

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
		entry.loggerEntry.Info(fmt.Sprintf("Connecting to database [%s] success", innerDb.name))
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
