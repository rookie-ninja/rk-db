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
	"github.com/rookie-ninja/rk-entry/v2/entry"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
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
	Database    []struct {
		Name                 string   `yaml:"name" json:"name"`
		Params               []string `yaml:"params" json:"params"`
		DryRun               bool     `yaml:"dryRun" json:"dryRun"`
		AutoCreate           bool     `yaml:"autoCreate" json:"autoCreate"`
		PreferSimpleProtocol bool     `yaml:"preferSimpleProtocol" json:"preferSimpleProtocol"`
	} `yaml:"database" json:"database"`
	LoggerEntry string `yaml:"loggerEntry" json:"loggerEntry"`
}

// PostgresEntry will init gorm.DB with provided arguments
type PostgresEntry struct {
	entryName        string                  `yaml:"entryName" json:"entryName"`
	entryType        string                  `yaml:"entryType" json:"entryType"`
	entryDescription string                  `yaml:"-" json:"-"`
	User             string                  `yaml:"user" json:"user"`
	pass             string                  `yaml:"-" json:"-"`
	loggerEntry      *rkentry.LoggerEntry    `yaml:"-" json:"-"`
	Addr             string                  `yaml:"addr" json:"addr"`
	innerDbList      []*databaseInner        `yaml:"-" json:"-"`
	GormDbMap        map[string]*gorm.DB     `yaml:"-" json:"-"`
	GormConfigMap    map[string]*gorm.Config `yaml:"-" json:"-"`
}

type databaseInner struct {
	name                 string
	dryRun               bool
	autoCreate           bool
	preferSimpleProtocol bool
	params               []string
}

// Option will be extended in future.
type Option func(*PostgresEntry)

// WithName provide name.
func WithName(name string) Option {
	return func(entry *PostgresEntry) {
		entry.entryName = name
	}
}

// WithDescription provide name.
func WithDescription(description string) Option {
	return func(entry *PostgresEntry) {
		entry.entryDescription = description
	}
}

// WithUser provide user
func WithUser(user string) Option {
	return func(m *PostgresEntry) {
		if len(user) > 0 {
			m.User = user
		}
	}
}

// WithPass provide password
func WithPass(pass string) Option {
	return func(m *PostgresEntry) {
		if len(pass) > 0 {
			m.pass = pass
		}
	}
}

// WithAddr provide address
func WithAddr(addr string) Option {
	return func(m *PostgresEntry) {
		if len(addr) > 0 {
			m.Addr = addr
		}
	}
}

// WithDatabase provide database
func WithDatabase(name string, dryRun, autoCreate, preferSimpleProtocol bool, params ...string) Option {
	return func(m *PostgresEntry) {
		if len(name) < 1 {
			return
		}

		innerDb := &databaseInner{
			name:                 name,
			dryRun:               dryRun,
			autoCreate:           autoCreate,
			preferSimpleProtocol: preferSimpleProtocol,
			params:               make([]string, 0),
		}

		// add default params if no param provided
		if len(params) < 1 {
			innerDb.params = append(innerDb.params,
				"sslmode=disable",
				"TimeZone=Asia/Shanghai")
		} else {
			innerDb.params = append(innerDb.params, params...)
		}

		m.innerDbList = append(m.innerDbList, innerDb)
	}
}

// WithLoggerEntry provide rkentry.LoggerEntry entry name
func WithLoggerEntry(entry *rkentry.LoggerEntry) Option {
	return func(m *PostgresEntry) {
		if entry != nil {
			m.loggerEntry = entry
		}
	}
}

// RegisterPostgresEntryYAML register PostgresEntry based on config file into rkentry.GlobalAppCtx
func RegisterPostgresEntryYAML(raw []byte) map[string]rkentry.Entry {
	res := make(map[string]rkentry.Entry)

	// 1: unmarshal user provided config into boot config struct
	config := &BootPostgres{}
	rkentry.UnmarshalBootYAML(raw, config)

	// filter out based domain
	configMap := make(map[string]*BootPostgresE)
	for _, e := range config.Postgres {
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
			WithUser(element.User),
			WithPass(element.Pass),
			WithAddr(element.Addr),
			WithLoggerEntry(rkentry.GlobalAppCtx.GetLoggerEntry(element.LoggerEntry)),
		}

		// iterate database section
		for _, db := range element.Database {
			opts = append(opts, WithDatabase(db.Name, db.DryRun, db.AutoCreate, db.PreferSimpleProtocol, db.Params...))
		}

		entry := RegisterPostgresEntry(opts...)

		res[element.Name] = entry
	}

	return res
}

// RegisterPostgresEntry will register Entry into GlobalAppCtx
func RegisterPostgresEntry(opts ...Option) *PostgresEntry {
	entry := &PostgresEntry{
		entryName:        "PostgreSQL",
		entryType:        PostgreSqlEntry,
		entryDescription: "PostgreSQL entry for gorm.DB",
		User:             "postgres",
		pass:             "pass",
		Addr:             "localhost:5432",
		innerDbList:      make([]*databaseInner, 0),
		loggerEntry:      rkentry.NewLoggerEntryStdout(),
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
				SlowThreshold:             5000 * time.Millisecond,
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

	entry.loggerEntry.Info("Bootstrap postgresEntry", fields...)

	// Connect and create db if missing
	if err := entry.connect(); err != nil {
		fields = append(fields, zap.Error(err))
		entry.loggerEntry.Error("Failed to connect to database", fields...)
		rkentry.ShutdownWithError(fmt.Errorf("failed to connect to database at %s@%s",
			entry.User, entry.Addr))
	}
}

// Interrupt PostgresEntry
func (entry *PostgresEntry) Interrupt(ctx context.Context) {
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

	entry.loggerEntry.Info("Interrupt PostgresEntry", fields...)
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
			return false
		} else {
			if err := db.Ping(); err != nil {
				return false
			}
		}
	}

	return true
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
			entry.loggerEntry.Info(fmt.Sprintf("Creating database [%s] if not exists", innerDb.name))

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
				return err
			}

			// 2: check if db exists with bellow statement
			innerDbInfo := make(map[string]interface{})
			res := db.Raw("SELECT * FROM pg_database WHERE datname = ?", innerDb.name).Scan(innerDbInfo)

			if res.Error != nil {
				return res.Error
			}

			// 3: database not found, create one
			if len(innerDbInfo) < 1 {
				entry.loggerEntry.Info(fmt.Sprintf("Database:%s not found, create with owner:%s, encoding:UTF8", innerDb.name, entry.User))
				res := db.Exec(fmt.Sprintf(`CREATE DATABASE "%s" WITH OWNER %s ENCODING %s`, innerDb.name, entry.User, "UTF8"))
				if res.Error != nil {
					return res.Error
				}
			}

			entry.loggerEntry.Info(fmt.Sprintf("Creating database [%s] successs", innerDb.name))
		}

		entry.loggerEntry.Info(fmt.Sprintf("Connecting to database [%s]", innerDb.name))

		// 2: connect
		params = append(params, fmt.Sprintf("dbname=%s", innerDb.name))
		dsn := strings.Join(params, " ")

		db, err = gorm.Open(postgres.Open(dsn), entry.GormConfigMap[innerDb.name])

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

// GetPostgresEntry returns PostgresEntry instance
func GetPostgresEntry(name string) *PostgresEntry {
	if raw := rkentry.GlobalAppCtx.GetEntry(PostgreSqlEntry, name); raw != nil {
		if entry, ok := raw.(*PostgresEntry); ok {
			return entry
		}
	}

	return nil
}
