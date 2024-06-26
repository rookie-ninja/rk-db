// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.

// Package rkpostgres is an implementation of rkentry.Entry which could be used gorm.DB instance.
package rkpostgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rookie-ninja/rk-db/postgres/plugins"
	"github.com/rookie-ninja/rk-entry/v2/entry"
	"github.com/rookie-ninja/rk-logger"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
	"os"
	"path"
	"strings"
	"time"
)

// This must be declared in order to register registration function into rk context
// otherwise, rk-boot won't able to bootstrap echo entry automatically from boot config file
func init() {
	rkentry.RegisterPluginRegFunc(RegisterPostgresEntryYAML)
}

const PostgreSqlEntry = "PostgreSqlEntry"

// BootPostgres
// Postgres entry boot config which reflects to YAML config
type BootPostgres struct {
	Postgres []*BootPostgresE `yaml:"postgres" json:"postgres"`
}

type BootPostgresE struct {
	Enabled     bool   `yaml:"enabled" json:"enabled"`
	Name        string `yaml:"name" json:"name"`
	Description string `yaml:"description" json:"description"`
	Domain      string `yaml:"domain" json:"domain"`
	User        string `yaml:"user" json:"user"`
	Pass        string `yaml:"pass" json:"pass"`
	Addr        string `yaml:"addr" json:"addr"`
	HealthCheck struct {
		Enabled    bool `json:"enabled"`
		IntervalMs int  `json:"intervalMs"`
	} `json:"healthCheck"`
	Database []struct {
		Name                 string   `yaml:"name" json:"name"`
		Params               []string `yaml:"params" json:"params"`
		DryRun               bool     `yaml:"dryRun" json:"dryRun"`
		AutoCreate           bool     `yaml:"autoCreate" json:"autoCreate"`
		PreferSimpleProtocol bool     `yaml:"preferSimpleProtocol" json:"preferSimpleProtocol"`
		MaxIdleConn          int      `yaml:"maxIdleConn" json:"maxIdleConn"`
		MaxOpenConn          int      `yaml:"maxOpenConn" json:"maxOpenConn"`
		Plugins              struct {
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

// PostgresEntry will init gorm.DB with provided arguments
type PostgresEntry struct {
	entryName           string                  `yaml:"entryName" json:"entryName"`
	entryType           string                  `yaml:"entryType" json:"entryType"`
	entryDescription    string                  `yaml:"-" json:"-"`
	User                string                  `yaml:"user" json:"user"`
	pass                string                  `yaml:"-" json:"-"`
	logger              *Logger                 `yaml:"-" json:"-"`
	Addr                string                  `yaml:"addr" json:"addr"`
	innerDbList         []*databaseInner        `yaml:"-" json:"-"`
	GormDbMap           map[string]*gorm.DB     `yaml:"-" json:"-"`
	GormConfigMap       map[string]*gorm.Config `yaml:"-" json:"-"`
	quitChannel         chan struct{}           `yaml:"-" json:"-"`
	healthCheckEnabled  bool                    `yaml:"-" json:"-"`
	healthCheckInterval time.Duration           `yaml:"-" json:"-"`
}

type databaseInner struct {
	name                 string
	dryRun               bool
	autoCreate           bool
	preferSimpleProtocol bool
	maxIdleConn          int
	maxOpenConn          int
	params               []string
	plugins              []gorm.Plugin
}

// RegisterPostgresEntryYAML register PostgresEntry based on config file into rkentry.GlobalAppCtx
func RegisterPostgresEntryYAML(raw []byte) map[string]rkentry.Entry {
	// 1: unmarshal user provided config into boot config struct
	config := &BootPostgres{}
	rkentry.UnmarshalBootYAML(raw, config)

	res := make(map[string]rkentry.Entry)

	entries := RegisterPostgresEntry(config)
	for i := range entries {
		entry := entries[i]
		res[entry.GetName()] = entry
	}

	return res
}

func RegisterPostgresEntry(boot *BootPostgres) []*PostgresEntry {
	res := make([]*PostgresEntry, 0)

	// filter out based domain
	configMap := make(map[string]*BootPostgresE)
	for _, e := range boot.Postgres {
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

		entry := &PostgresEntry{
			entryName:     element.Name,
			entryType:     PostgreSqlEntry,
			User:          element.User,
			pass:          element.Pass,
			Addr:          element.Addr,
			innerDbList:   make([]*databaseInner, 0),
			GormDbMap:     make(map[string]*gorm.DB),
			GormConfigMap: make(map[string]*gorm.Config),
			logger:        logger,
			quitChannel:   make(chan struct{}),
		}

		if element.HealthCheck.Enabled {
			entry.healthCheckEnabled = true
			if element.HealthCheck.IntervalMs > 0 {
				entry.healthCheckInterval = time.Duration(element.HealthCheck.IntervalMs * int(time.Millisecond))
			} else {
				entry.healthCheckInterval = 5000 * time.Millisecond
			}
		}

		// iterate database section
		for _, db := range element.Database {
			// init inner db
			if len(db.Name) < 1 {
				continue
			}

			innerDb := &databaseInner{
				name:                 db.Name,
				dryRun:               db.DryRun,
				autoCreate:           db.AutoCreate,
				preferSimpleProtocol: db.PreferSimpleProtocol,
				params:               make([]string, 0),
			}

			// add default params if no param provided
			if len(db.Params) < 1 {
				innerDb.params = append(innerDb.params,
					"sslmode=disable",
					"TimeZone=Asia/Shanghai")
			} else {
				innerDb.params = append(innerDb.params, db.Params...)
			}

			entry.innerDbList = append(entry.innerDbList, innerDb)

			if db.Plugins.Prom.Enabled {
				db.Plugins.Prom.DbAddr = element.Addr
				db.Plugins.Prom.DbName = db.Name
				db.Plugins.Prom.DbType = "postgresql"
				prom := plugins.NewProm(&db.Plugins.Prom)
				innerDb.plugins = append(innerDb.plugins, prom)
			}
		}

		if len(entry.User) < 1 {
			entry.User = "postgres"
		}
		if len(entry.pass) < 1 {
			entry.pass = "pass"
		}
		if len(entry.Addr) < 1 {
			entry.Addr = "localhost:5432"
		}
		if len(entry.entryDescription) < 1 {
			entry.entryDescription = fmt.Sprintf("%s entry with name of %s, addr:%s, user:%s",
				entry.entryType,
				entry.entryName,
				entry.Addr,
				entry.User)
		}
		for _, innerDb := range entry.innerDbList {
			entry.GormConfigMap[innerDb.name] = &gorm.Config{
				Logger: entry.logger,
				DryRun: innerDb.dryRun,
			}
		}

		rkentry.GlobalAppCtx.AddEntry(entry)
		res = append(res, entry)
	}

	return res
}

// Bootstrap PostgresEntry
func (entry *PostgresEntry) Bootstrap(ctx context.Context) {
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

	entry.logger.delegate.Info("Bootstrap postgresEntry", fields...)

	// Connect and create db if missing
	if err := entry.connect(); err != nil {
		fields = append(fields, zap.Error(err))
		entry.logger.delegate.Error("Failed to connect to database", fields...)
		rkentry.ShutdownWithError(fmt.Errorf("failed to connect to database at %s@%s",
			entry.User, entry.Addr))
	}

	// enable health check
	if entry.healthCheckEnabled {
		go func() {
			waitChannel := time.NewTimer(entry.healthCheckInterval)

			for {
				select {
				case <-entry.quitChannel:
					return
				case <-waitChannel.C:
					entry.IsHealthy()
					waitChannel.Reset(entry.healthCheckInterval)
				default:
					time.Sleep(time.Duration(3) * time.Second)
				}
			}
		}()
	}
}

// Interrupt PostgresEntry
func (entry *PostgresEntry) Interrupt(ctx context.Context) {
	close(entry.quitChannel)

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

	entry.logger.delegate.Info("Interrupt PostgresEntry", fields...)
}

// GetName returns entry name
func (entry *PostgresEntry) GetName() string {
	return entry.entryName
}

// GetType returns entry type
func (entry *PostgresEntry) GetType() string {
	return entry.entryType
}

// GetDescription returns entry description
func (entry *PostgresEntry) GetDescription() string {
	return entry.entryDescription
}

// String returns json marshalled string
func (entry *PostgresEntry) String() string {
	bytes, err := json.Marshal(entry)
	if err != nil || len(bytes) < 1 {
		return "{}"
	}

	return string(bytes)
}

// IsHealthy checks healthy status remote provider
func (entry *PostgresEntry) IsHealthy() bool {
	for _, gormDb := range entry.GormDbMap {
		if db, err := gormDb.DB(); err != nil {
			entry.logger.delegate.Warn("failed get DB", zap.String("db", gormDb.Name()))
			return false
		} else {
			if err := db.Ping(); err != nil {
				entry.logger.delegate.Warn("failed to ping DB", zap.String("db", gormDb.Name()))
				return false
			}
		}
	}

	return true
}

func (entry *PostgresEntry) RegisterPromMetrics(registry *prometheus.Registry) error {
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

func (entry *PostgresEntry) GetDB(name string) *gorm.DB {
	return entry.GormDbMap[name]
}

// Create database if missing
func (entry *PostgresEntry) connect() error {
	// 1: parse address to port and host
	tokens := strings.Split(entry.Addr, ":")
	if len(tokens) != 2 {
		return errors.New("invalid address, should be format of localhost:9920")
	}

	host := tokens[0]
	port := tokens[1]

	dsnParams := []string{
		fmt.Sprintf("host=%s", host),
		fmt.Sprintf("port=%s", port),
		fmt.Sprintf("user=%s", entry.User),
		fmt.Sprintf("password=%s", entry.pass)}

	for _, innerDb := range entry.innerDbList {
		var db *gorm.DB
		var err error

		params := make([]string, 0)
		params = append(params, dsnParams...)
		params = append(params, innerDb.params...)

		// 1: create db if missing
		if !innerDb.dryRun && innerDb.autoCreate {
			entry.logger.delegate.Info(fmt.Sprintf("Creating database [%s] if not exists", innerDb.name))

			// It is a little bit complex procedure here
			// connect to database postgres and try to create DB
			paramsForDefaultDb := make([]string, 0)
			paramsForDefaultDb = append(paramsForDefaultDb, params...)
			paramsForDefaultDb = append(paramsForDefaultDb, "dbname=postgres")

			dsnForDefaultDb := strings.Join(paramsForDefaultDb, " ")

			// 1: connect to db postgres
			db, err = gorm.Open(postgres.Open(dsnForDefaultDb), entry.GormConfigMap[innerDb.name])
			// failed to connect to database
			if err != nil {
				closeDB(db)
				return err
			}

			// 2: check if db exists with bellow statement
			innerDbInfo := make(map[string]interface{})
			res := db.Raw("SELECT * FROM pg_database WHERE datname = ?", innerDb.name).Scan(innerDbInfo)

			if res.Error != nil {
				closeDB(db)
				return res.Error
			}

			// 3: database not found, create one
			if len(innerDbInfo) < 1 {
				entry.logger.delegate.Info(fmt.Sprintf("Database:%s not found, create with owner:%s, encoding:UTF8", innerDb.name, entry.User))
				res := db.Exec(fmt.Sprintf(`CREATE DATABASE "%s" WITH OWNER %s ENCODING %s`, innerDb.name, entry.User, "UTF8"))
				if res.Error != nil {
					closeDB(db)
					return res.Error
				}
			}

			closeDB(db)
			entry.logger.delegate.Info(fmt.Sprintf("Creating database [%s] successs", innerDb.name))
		}

		entry.logger.delegate.Info(fmt.Sprintf("Connecting to database [%s]", innerDb.name))

		// 2: connect
		params = append(params, fmt.Sprintf("dbname=%s", innerDb.name))
		dsn := strings.Join(params, " ")

		db, err = gorm.Open(postgres.Open(dsn), entry.GormConfigMap[innerDb.name])

		// failed to connect to database
		if err != nil {
			return err
		}

		if innerDb.maxOpenConn > 0 {
			if inner, err := db.DB(); err != nil {
				return err
			} else {
				inner.SetMaxOpenConns(innerDb.maxOpenConn)
			}
		}

		if innerDb.maxIdleConn > 0 {
			if inner, err := db.DB(); err != nil {
				return err
			} else {
				inner.SetMaxIdleConns(innerDb.maxIdleConn)
			}
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

// GetPostgresEntry returns PostgresEntry instance
func GetPostgresEntry(name string) *PostgresEntry {
	if raw := rkentry.GlobalAppCtx.GetEntry(PostgreSqlEntry, name); raw != nil {
		if entry, ok := raw.(*PostgresEntry); ok {
			return entry
		}
	}

	return nil
}

// Make incoming paths to absolute path with current working directory attached as prefix
func toAbsPath(p ...string) []string {
	res := make([]string, 0)

	for i := range p {
		if path.IsAbs(p[i]) || p[i] == "stdout" || p[i] == "stderr" {
			res = append(res, p[i])
			continue
		}
		wd, _ := os.Getwd()
		res = append(res, path.Join(wd, p[i]))
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
