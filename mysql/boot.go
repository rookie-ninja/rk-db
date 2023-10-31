// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.

// Package rkmysql is an implementation of rkentry.Entry which could be used gorm.DB instance.
package rkmysql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rookie-ninja/rk-db/mysql/plugins"
	"github.com/rookie-ninja/rk-entry/v2/entry"
	"github.com/rookie-ninja/rk-logger"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
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
	rkentry.RegisterPluginRegFunc(RegisterMySqlEntryYAML)
}

const MySqlEntryType = "MySqlEntry"

// BootMySQL
// MySql entry boot config which reflects to YAML config
type BootMySQL struct {
	MySql []*BootMySQLE `yaml:"mysql" json:"mysql"`
}

type BootMySQLE struct {
	Enabled     bool   `yaml:"enabled" json:"enabled"`
	Name        string `yaml:"name" json:"name"`
	Description string `yaml:"description" json:"description"`
	Domain      string `yaml:"domain" json:"domain"`
	User        string `yaml:"user" json:"user"`
	Pass        string `yaml:"pass" json:"pass"`
	Protocol    string `yaml:"protocol" json:"protocol"`
	Addr        string `yaml:"addr" json:"addr"`
	Database    []struct {
		Name       string   `yaml:"name" json:"name"`
		Params     []string `yaml:"params" json:"params"`
		DryRun     bool     `yaml:"dryRun" json:"dryRun"`
		AutoCreate bool     `yaml:"autoCreate" json:"autoCreate"`
		Plugins    struct {
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

// MySqlEntry will init gorm.DB or SqlMock with provided arguments
type MySqlEntry struct {
	entryName        string                  `yaml:"entryName" yaml:"entryName"`
	entryType        string                  `yaml:"entryType" yaml:"entryType"`
	entryDescription string                  `yaml:"-" json:"-"`
	User             string                  `yaml:"user" json:"user"`
	pass             string                  `yaml:"-" json:"-"`
	logger           *Logger                 `yaml:"-" json:"-"`
	Protocol         string                  `yaml:"protocol" json:"protocol"`
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
	plugins    []gorm.Plugin
}

// Option for MySqlEntry
type Option func(*MySqlEntry)

// WithName provide name.
func WithName(name string) Option {
	return func(entry *MySqlEntry) {
		entry.entryName = name
	}
}

// WithDescription provide name.
func WithDescription(description string) Option {
	return func(entry *MySqlEntry) {
		entry.entryDescription = description
	}
}

// WithUser provide user
func WithUser(user string) Option {
	return func(m *MySqlEntry) {
		if len(user) > 0 {
			m.User = user
		}
	}
}

// WithPass provide password
func WithPass(pass string) Option {
	return func(m *MySqlEntry) {
		if len(pass) > 0 {
			m.pass = pass
		}
	}
}

// WithProtocol provide protocol
func WithProtocol(protocol string) Option {
	return func(m *MySqlEntry) {
		if len(protocol) > 0 {
			m.Protocol = protocol
		}
	}
}

// WithAddr provide address
func WithAddr(addr string) Option {
	return func(m *MySqlEntry) {
		if len(addr) > 0 {
			m.Addr = addr
		}
	}
}

// WithDatabase provide database
func WithDatabase(name string, dryRun, autoCreate bool, params ...string) Option {
	return func(m *MySqlEntry) {
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
		if len(params) < 1 {
			innerDb.params = append(innerDb.params,
				"charset=utf8mb4",
				"parseTime=True",
				"loc=Local")
		} else {
			innerDb.params = append(innerDb.params, params...)
		}

		m.innerDbList = append(m.innerDbList, innerDb)
	}
}

func WithPlugin(name string, plugin gorm.Plugin) Option {
	return func(entry *MySqlEntry) {
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
	return func(m *MySqlEntry) {
		if logger != nil {
			m.logger = logger
		}
	}
}

// RegisterMySqlEntryYAML register MySqlEntry based on config file into rkentry.GlobalAppCtx
func RegisterMySqlEntryYAML(raw []byte) map[string]rkentry.Entry {
	res := make(map[string]rkentry.Entry)

	// 1: unmarshal user provided config into boot config struct
	config := &BootMySQL{}
	rkentry.UnmarshalBootYAML(raw, config)

	// filter out based domain
	configMap := make(map[string]*BootMySQLE)
	for _, e := range config.MySql {
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
			WithUser(element.User),
			WithPass(element.Pass),
			WithProtocol(element.Protocol),
			WithAddr(element.Addr),
			WithLogger(logger),
		}

		// iterate database section
		for _, db := range element.Database {
			opts = append(opts, WithDatabase(db.Name, db.DryRun, db.AutoCreate, db.Params...))

			if db.Plugins.Prom.Enabled {
				db.Plugins.Prom.DbAddr = element.Addr
				db.Plugins.Prom.DbName = db.Name
				db.Plugins.Prom.DbType = "mysql"
				prom := plugins.NewProm(&db.Plugins.Prom)
				opts = append(opts, WithPlugin(db.Name, prom))
			}
		}

		entry := RegisterMySqlEntry(opts...)

		res[element.Name] = entry
	}

	return res
}

// RegisterMySqlEntry will register Entry into GlobalAppCtx
func RegisterMySqlEntry(opts ...Option) *MySqlEntry {
	entry := &MySqlEntry{
		entryName:        "MySql",
		entryType:        MySqlEntryType,
		entryDescription: "MySql entry for gorm.DB",
		User:             "root",
		pass:             "pass",
		Protocol:         "tcp",
		Addr:             "localhost:3306",
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
			Logger: entry.logger,
			DryRun: innerDb.dryRun,
		}
	}

	rkentry.GlobalAppCtx.AddEntry(entry)

	return entry
}

// Bootstrap MySqlEntry
func (entry *MySqlEntry) Bootstrap(ctx context.Context) {
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

	entry.logger.delegate.Info("Bootstrap MySqlEntry", fields...)

	// Connect and create db if missing
	if err := entry.connect(); err != nil {
		fields = append(fields, zap.Error(err))
		entry.logger.delegate.Error("Failed to connect to database", fields...)
		rkentry.ShutdownWithError(fmt.Errorf("failed to connect to database at %s:%s@%s(%s)",
			entry.User, "****", entry.Protocol, entry.Addr))
	}
}

// Interrupt MySqlEntry
func (entry *MySqlEntry) Interrupt(ctx context.Context) {
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

	entry.logger.delegate.Info("Interrupt MySqlEntry", fields...)
}

// GetName returns entry name
func (entry *MySqlEntry) GetName() string {
	return entry.entryName
}

// GetType returns entry type
func (entry *MySqlEntry) GetType() string {
	return entry.entryType
}

// GetDescription returns entry description
func (entry *MySqlEntry) GetDescription() string {
	return entry.entryDescription
}

// String returns json marshalled string
func (entry *MySqlEntry) String() string {
	bytes, err := json.Marshal(entry)
	if err != nil || len(bytes) < 1 {
		return "{}"
	}

	return string(bytes)
}

// IsHealthy checks healthy status remote provider
func (entry *MySqlEntry) IsHealthy() bool {
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

func (entry *MySqlEntry) RegisterPromMetrics(registry *prometheus.Registry) error {
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

func (entry *MySqlEntry) GetDB(name string) *gorm.DB {
	return entry.GormDbMap[name]
}

// Create database if missing
func (entry *MySqlEntry) connect() error {
	for _, innerDb := range entry.innerDbList {
		var db *gorm.DB
		var err error

		sqlParams := strings.Join(innerDb.params, "&")

		// 1: create db if missing
		if !innerDb.dryRun && innerDb.autoCreate {
			entry.logger.delegate.Info(fmt.Sprintf("Creating database [%s]", innerDb.name))

			dsn := fmt.Sprintf("%s:%s@%s(%s)/?%s",
				entry.User, entry.pass, entry.Protocol, entry.Addr, sqlParams)

			db, err = gorm.Open(mysql.Open(dsn), entry.GormConfigMap[innerDb.name])

			// failed to connect to database
			if err != nil {
				return err
			}

			createSQL := fmt.Sprintf(
				"CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4;",
				innerDb.name,
			)

			db = db.Exec(createSQL)

			if db.Error != nil {
				return db.Error
			}
			entry.logger.delegate.Info(fmt.Sprintf("Creating database [%s] successs", innerDb.name))
		}

		entry.logger.delegate.Info(fmt.Sprintf("Connecting to database [%s]", innerDb.name))
		dsn := fmt.Sprintf("%s:%s@%s(%s)/%s?%s",
			entry.User, entry.pass, entry.Protocol, entry.Addr, innerDb.name, sqlParams)

		db, err = gorm.Open(mysql.Open(dsn), entry.GormConfigMap[innerDb.name])

		// failed to connect to database
		if err != nil {
			return err
		}

		for i := range innerDb.plugins {
			if err := db.Use(innerDb.plugins[i]); err != nil {
				return err
			}
		}

		entry.GormDbMap[innerDb.name] = db
		entry.logger.delegate.Info(fmt.Sprintf("Connecting to database [%s] success", innerDb.name))
	}

	return nil
}

// GetMySqlEntry returns MySqlEntry instance
func GetMySqlEntry(name string) *MySqlEntry {
	if raw := rkentry.GlobalAppCtx.GetEntry(MySqlEntryType, name); raw != nil {
		if entry, ok := raw.(*MySqlEntry); ok {
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
