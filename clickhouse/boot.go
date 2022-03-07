// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.

// Package rkclickhouse is an implementation of rkentry.Entry which could be used gorm.DB instance.
package rkclickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rookie-ninja/rk-entry/v2/entry"
	"go.uber.org/zap"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"strings"
	"time"
)

const ClickHouseEntryType = "ClickHouseEntry"

// This must be declared in order to register registration function into rk context
// otherwise, rk-boot won't able to bootstrap echo entry automatically from boot config file
func init() {
	rkentry.RegisterEntryRegFunc(RegisterClickHouseEntryYAML)
}

// BootConfig
// ClickHouse entry boot config which reflects to YAML config
type BootConfig struct {
	ClickHouse []struct {
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
		LoggerEntry string `yaml:"loggerEntry" json:"loggerEntry"`
	} `yaml:"clickhouse" json:"clickhouse"`
}

// ClickHouseEntry will init gorm.DB or SqlMock with provided arguments
type ClickHouseEntry struct {
	entryName        string                  `yaml:"-" yaml:"-"`
	entryType        string                  `yaml:"-" yaml:"-"`
	entryDescription string                  `yaml:"-" json:"-"`
	User             string                  `yaml:"-" json:"-"`
	pass             string                  `yaml:"-" json:"-"`
	loggerEntry      *rkentry.LoggerEntry    `yaml:"-" json:"-"`
	Addr             string                  `yaml:"-" json:"-"`
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

type Option func(*ClickHouseEntry)

// WithName provide name.
func WithName(name string) Option {
	return func(entry *ClickHouseEntry) {
		entry.entryName = name
	}
}

// WithDescription provide name.
func WithDescription(description string) Option {
	return func(entry *ClickHouseEntry) {
		entry.entryDescription = description
	}
}

// WithUser provide user
func WithUser(user string) Option {
	return func(m *ClickHouseEntry) {
		if len(user) > 0 {
			m.User = user
		}
	}
}

// WithPass provide password
func WithPass(pass string) Option {
	return func(m *ClickHouseEntry) {
		if len(pass) > 0 {
			m.pass = pass
		}
	}
}

// WithAddr provide address
func WithAddr(addr string) Option {
	return func(m *ClickHouseEntry) {
		if len(addr) > 0 {
			m.Addr = addr
		}
	}
}

// WithDatabase provide database
func WithDatabase(name string, dryRun, autoCreate bool, params ...string) Option {
	return func(m *ClickHouseEntry) {
		if len(name) < 1 {
			return
		}

		innerDb := &databaseInner{
			name:       name,
			dryRun:     dryRun,
			autoCreate: autoCreate,
			params:     make([]string, 0),
		}

		// add default params if no param provided
		innerDb.params = append(innerDb.params, params...)

		m.innerDbList = append(m.innerDbList, innerDb)
	}
}

// WithLoggerEntry provide rkentry.ZapLoggerEntry entry name
func WithLoggerEntry(entry *rkentry.LoggerEntry) Option {
	return func(m *ClickHouseEntry) {
		if entry != nil {
			m.loggerEntry = entry
		}
	}
}

// RegisterClickHouseEntryYAML register ClickHouseEntry based on config file into rkentry.GlobalAppCtx
func RegisterClickHouseEntryYAML(raw []byte) map[string]rkentry.Entry {
	res := make(map[string]rkentry.Entry)

	// 1: unmarshal user provided config into boot config struct
	config := &BootConfig{}
	rkentry.UnmarshalBootYAML(raw, config)

	for _, element := range config.ClickHouse {
		if len(element.Name) < 1 || !rkentry.IsLocaleValid(element.Locale) {
			continue
		}

		opts := []Option{
			WithName(element.Name),
			WithDescription(element.Description),
			WithUser(element.User),
			WithPass(element.Pass),
			WithAddr(element.Addr),
			WithLoggerEntry(rkentry.GlobalAppCtx.GetLoggerEntry(element.LoggerEntry)),
		}

		// iterate database section
		for _, db := range element.Database {
			opts = append(opts, WithDatabase(db.Name, db.DryRun, db.AutoCreate, db.Params...))
		}

		entry := RegisterClickHouseEntry(opts...)

		res[element.Name] = entry
	}

	return res
}

// RegisterClickHouseEntry will register Entry into GlobalAppCtx
func RegisterClickHouseEntry(opts ...Option) *ClickHouseEntry {
	entry := &ClickHouseEntry{
		entryName:        "ClickHouse",
		entryType:        ClickHouseEntryType,
		entryDescription: "ClickHouse entry for gorm.DB",
		User:             "default",
		pass:             "",
		Addr:             "localhost:9000",
		innerDbList:      make([]*databaseInner, 0),
		loggerEntry:      rkentry.LoggerEntryStdout,
		GormDbMap:        make(map[string]*gorm.DB),
		GormConfigMap:    make(map[string]*gorm.Config),
	}

	for i := range opts {
		opts[i](entry)
	}

	if len(entry.entryDescription) < 1 {
		entry.entryDescription = fmt.Sprintf("%s entry with name of %s, addr:%s, user:%s",
			entry.entryType,
			entry.entryName,
			entry.Addr,
			entry.User)
	}

	// create default gorm configs for databases
	for _, innerDb := range entry.innerDbList {
		entry.GormConfigMap[innerDb.name] = &gorm.Config{
			Logger: logger.New(NewLogger(entry.loggerEntry.Logger), logger.Config{
				SlowThreshold:             5 * time.Second,
				LogLevel:                  logger.Warn,
				IgnoreRecordNotFoundError: false,
				Colorful:                  false,
			}),
			DryRun: innerDb.dryRun,
		}
	}

	rkentry.GlobalAppCtx.AddEntry(entry)

	return entry
}

// Bootstrap ClickHouseEntry
func (entry *ClickHouseEntry) Bootstrap(ctx context.Context) {
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

	entry.loggerEntry.Info("Bootstrap clickHouseEntry", fields...)

	// Connect and create db if missing
	if err := entry.connect(); err != nil {
		fields = append(fields, zap.Error(err))
		entry.loggerEntry.Error("Failed to connect to database", fields...)
		rkentry.ShutdownWithError(fmt.Errorf("failed to connect to database at %s:%s@%s",
			entry.User, "****", entry.Addr))
	}
}

// Interrupt ClickHouseEntry
func (entry *ClickHouseEntry) Interrupt(ctx context.Context) {
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

	entry.loggerEntry.Info("Interrupt clickHouseEntry", fields...)
}

// GetName returns entry name
func (entry *ClickHouseEntry) GetName() string {
	return entry.entryName
}

// GetType returns entry type
func (entry *ClickHouseEntry) GetType() string {
	return entry.entryType
}

// GetDescription returns entry description
func (entry *ClickHouseEntry) GetDescription() string {
	return entry.entryDescription
}

// String returns json marshalled string
func (entry *ClickHouseEntry) String() string {
	bytes, err := json.Marshal(entry)
	if err != nil || len(bytes) < 1 {
		return "{}"
	}

	return string(bytes)
}

// IsHealthy checks healthy status remote provider
func (entry *ClickHouseEntry) IsHealthy() bool {
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

// GetDB returns gorm.DB
func (entry *ClickHouseEntry) GetDB(name string) *gorm.DB {
	return entry.GormDbMap[name]
}

// Create database if missing
func (entry *ClickHouseEntry) connect() error {
	for _, innerDb := range entry.innerDbList {
		var db *gorm.DB
		var err error

		credentialParams := []string{
			entry.User,
			entry.pass,
		}

		// CREATE DATABASE [IF NOT EXISTS] db_name

		// 1: create db if missing
		if !innerDb.dryRun && innerDb.autoCreate {
			entry.loggerEntry.Info(fmt.Sprintf("Creating database [%s]", innerDb.name))
			dsn := fmt.Sprintf("tcp://%s?%s", entry.Addr, strings.Join(credentialParams, "&"))

			db, err = gorm.Open(clickhouse.Open(dsn), entry.GormConfigMap[innerDb.name])

			// failed to connect to database
			if err != nil {
				return err
			}

			createSQL := fmt.Sprintf(
				"CREATE DATABASE IF NOT EXISTS %s",
				innerDb.name,
			)

			db = db.Exec(createSQL)

			if db.Error != nil {
				return db.Error
			}

			entry.loggerEntry.Info(fmt.Sprintf("Creating database [%s] successs", innerDb.name))
		}

		entry.loggerEntry.Info(fmt.Sprintf("Connecting to database [%s]", innerDb.name))
		params := []string{
			innerDb.name,
		}
		params = append(params, credentialParams...)
		params = append(params, innerDb.params...)

		dsn := fmt.Sprintf("tcp://%s?%s", entry.Addr, strings.Join(params, "&"))

		db, err = gorm.Open(clickhouse.Open(dsn), entry.GormConfigMap[innerDb.name])

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

// GetClickHouseEntry returns ClickHouseEntry instance
func GetClickHouseEntry(name string) *ClickHouseEntry {
	if raw := rkentry.GlobalAppCtx.GetEntry(ClickHouseEntryType, name); raw != nil {
		if entry, ok := raw.(*ClickHouseEntry); ok {
			return entry
		}
	}

	return nil
}
