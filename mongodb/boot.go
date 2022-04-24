// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.

// Package rkmongo is an implementation of rkentry.Entry which could be used mongo client instance.
package rkmongo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rookie-ninja/rk-entry/v2/entry"
	"go.mongodb.org/mongo-driver/mongo"
	mongoOpt "go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"strings"
	"time"
)

// This must be declared in order to register registration function into rk context
// otherwise, rk-boot won't able to bootstrap echo entry automatically from boot config file
func init() {
	rkentry.RegisterEntryRegFunc(RegisterMongoEntryYAML)
}

const MongoEntryType = "MongoEntry"

// GetMongoEntry returns MongoEntry
func GetMongoEntry(entryName string) *MongoEntry {
	if v := rkentry.GlobalAppCtx.GetEntry(MongoEntryType, entryName); v != nil {
		if res, ok := v.(*MongoEntry); ok {
			return res
		}
	}

	return nil
}

// GetMongoDB returns mongo.Database
func GetMongoDB(entryName, dbName string) *mongo.Database {
	if entry := GetMongoEntry(entryName); entry != nil {
		return entry.GetMongoDB(dbName)
	}

	return nil
}

// BootMongo
// MongoEntry boot config which reflects to YAML config
type BootMongo struct {
	Mongo []*BootMongoE `yaml:"mongo" json:"mongo"`
}

// BootMongoE sub struct for BootConfig
type BootMongoE struct {
	Name          string `yaml:"name" json:"name"`
	Enabled       bool   `yaml:"enabled" json:"enabled"`
	Description   string `yaml:"description" json:"description"`
	Domain        string `yaml:"domain" json:"domain"`
	SimpleURI     string `yaml:"simpleURI" json:"simpleURI"`
	PingTimeoutMs int    `yaml:"pingTimeoutMs" json:"pingTimeoutMs"`
	Database      []struct {
		Name string `yaml:"name" json:"name"`
	}
	LoggerEntry string  `yaml:"loggerEntry" json:"loggerEntry"`
	CertEntry   string  `yaml:"certEntry" json:"certEntry"`
	AppName     *string `yaml:"appName" json:"appName"`
	Auth        *struct {
		Mechanism           string            `yaml:"mechanism" json:"mechanism"`
		MechanismProperties map[string]string `yaml:"mechanismProperties" json:"mechanismProperties"`
		Source              string            `yaml:"source" json:"source"`
		Username            string            `yaml:"username" json:"username"`
		Password            string            `yaml:"password" json:"password"`
		PasswordSet         bool              `yaml:"passwordSet" json:"passwordSet"`
	} `yaml:"auth" json:"auth"`
	ConnectTimeoutMs         *int64   `yaml:"connectTimeoutMs" json:"connectTimeoutMs"`
	Compressors              []string `yaml:"compressors" json:"compressors"`
	Direct                   *bool    `yaml:"direct" json:"direct"`
	DisableOCSPEndpointCheck *bool    `yaml:"disableOCSPEndpointCheck" json:"disableOCSPEndpointCheck"`
	HeartbeatIntervalMs      *int64   `yaml:"heartbeatIntervalMs" json:"heartbeatIntervalMs"`
	Hosts                    []string `yaml:"hosts" json:"hosts"`
	LoadBalanced             *bool    `yaml:"loadBalanced" json:"loadBalanced"`
	LocalThresholdMs         *int64   `yaml:"localThresholdMs" json:"localThresholdMs"`
	MaxConnIdleTimeMs        *int64   `yaml:"maxConnIdleTimeMs" json:"maxConnIdleTimeMs"`
	MaxPoolSize              *uint64  `yaml:"maxPoolSize" json:"maxPoolSize"`
	MinPoolSize              *uint64  `yaml:"minPoolSize" json:"minPoolSize"`
	MaxConnecting            *uint64  `yaml:"maxConnecting" json:"maxConnecting"`
	ReplicaSet               *string  `yaml:"replicaSet" json:"replicaSet"`
	RetryReads               *bool    `yaml:"retryReads" json:"retryReads"`
	RetryWrites              *bool    `yaml:"retryWrites" json:"retryWrites"`
	ServerApiOptions         *struct {
		Version           string `yaml:"version" json:"version"`
		Strict            *bool  `yaml:"strict" json:"strict"`
		DeprecationErrors *bool  `yaml:"deprecationErrors" json:"deprecationErrors"`
	} `yaml:"serverApiOptions" json:"serverApiOptions"`
	ServerSelectionTimeoutMs *int    `yaml:"serverSelectionTimeoutMs" json:"serverSelectionTimeoutMs"`
	SocketTimeoutMs          *int    `yaml:"socketTimeoutMs" json:"socketTimeoutMs"`
	SRVMaxHosts              *int    `yaml:"srvMaxHosts" json:"srvMaxHosts"`
	SRVServiceName           *string `yaml:"srvServiceName" json:"srvServiceName"`
	ZlibLevel                *int    `yaml:"zlibLevel" json:"zlibLevel"`
	ZstdLevel                *int    `yaml:"zstdLevel" json:"zstdLevel"`
}

// ToClientOptions convert BootConfigMongo to options.ClientOptions
func ToClientOptions(config *BootMongoE) *mongoOpt.ClientOptions {
	if config == nil {
		return &mongoOpt.ClientOptions{}
	}

	opt := &mongoOpt.ClientOptions{
		AppName:                  config.AppName,
		Compressors:              config.Compressors,
		Direct:                   config.Direct,
		DisableOCSPEndpointCheck: config.DisableOCSPEndpointCheck,
		Hosts:                    config.Hosts,
		LoadBalanced:             config.LoadBalanced,
		MaxPoolSize:              config.MaxPoolSize,
		MinPoolSize:              config.MinPoolSize,
		MaxConnecting:            config.MaxConnecting,
		ReplicaSet:               config.ReplicaSet,
		RetryReads:               config.RetryReads,
		RetryWrites:              config.RetryWrites,
		SRVMaxHosts:              config.SRVMaxHosts,
		SRVServiceName:           config.SRVServiceName,
		ZlibLevel:                config.ZlibLevel,
		ZstdLevel:                config.ZstdLevel,
	}

	// auth
	if config.Auth != nil {
		opt.Auth = &mongoOpt.Credential{
			AuthMechanism:           config.Auth.Mechanism,
			AuthMechanismProperties: config.Auth.MechanismProperties,
			AuthSource:              config.Auth.Source,
			Username:                config.Auth.Username,
			Password:                config.Auth.Password,
			PasswordSet:             config.Auth.PasswordSet,
		}
	}

	// ConnectTimeout
	if config.ConnectTimeoutMs != nil {
		t := time.Duration(*config.ConnectTimeoutMs) * time.Millisecond
		opt.ConnectTimeout = &t
	}

	// HeartbeatInterval
	if config.HeartbeatIntervalMs != nil {
		t := time.Duration(*config.HeartbeatIntervalMs) * time.Millisecond
		opt.HeartbeatInterval = &t
	}

	// LocalThresholdMs
	if config.LocalThresholdMs != nil {
		t := time.Duration(*config.LocalThresholdMs) * time.Millisecond
		opt.LocalThreshold = &t
	}

	// MaxConnIdleTimeMs
	if config.MaxConnIdleTimeMs != nil {
		t := time.Duration(*config.MaxConnIdleTimeMs) * time.Millisecond
		opt.MaxConnIdleTime = &t
	}

	// ServerAPIOptions
	if config.ServerApiOptions != nil {
		opt.ServerAPIOptions = &mongoOpt.ServerAPIOptions{
			ServerAPIVersion:  mongoOpt.ServerAPIVersion(config.ServerApiOptions.Version),
			Strict:            config.ServerApiOptions.Strict,
			DeprecationErrors: config.ServerApiOptions.DeprecationErrors,
		}
	}

	// ServerSelectionTimeoutMs
	if config.ServerSelectionTimeoutMs != nil {
		t := time.Duration(*config.ServerSelectionTimeoutMs) * time.Millisecond
		opt.ServerSelectionTimeout = &t
	}

	// SocketTimeoutMs
	if config.SocketTimeoutMs != nil {
		t := time.Duration(*config.SocketTimeoutMs) * time.Millisecond
		opt.SocketTimeout = &t
	}

	// Apply simple URI, will overwrite the above configuration
	if config.SimpleURI != "" {
		opt = opt.ApplyURI(config.SimpleURI)
	}
	return opt
}

// RegisterMongoEntryYAML register MongoEntry based on config file into rkentry.GlobalAppCtx
func RegisterMongoEntryYAML(raw []byte) map[string]rkentry.Entry {
	res := make(map[string]rkentry.Entry)

	// 1: unmarshal user provided config into boot config struct
	config := &BootMongo{}
	rkentry.UnmarshalBootYAML(raw, config)

	// filter out based domain
	configMap := make(map[string]*BootMongoE)
	for _, e := range config.Mongo {
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
		if element.Enabled {
			clientOpt := ToClientOptions(element)

			certEntry := rkentry.GlobalAppCtx.GetCertEntry(element.CertEntry)

			opts := []Option{
				WithName(element.Name),
				WithDescription(element.Description),
				WithClientOptions(clientOpt),
				WithCertEntry(certEntry),
				WithPingTimeoutMs(element.PingTimeoutMs),
				WithLoggerEntry(rkentry.GlobalAppCtx.GetLoggerEntry(element.LoggerEntry)),
			}

			// iterate database
			for i := range element.Database {
				opts = append(opts, WithDatabase(element.Database[i].Name))
			}

			entry := RegisterMongoEntry(opts...)

			res[entry.GetName()] = entry
		}
	}

	return res
}

// RegisterMongoEntry will register Entry into GlobalAppCtx
func RegisterMongoEntry(opts ...Option) *MongoEntry {
	entry := &MongoEntry{
		entryName:        "MongoDB",
		entryType:        MongoEntryType,
		entryDescription: "Mongo entry for mongo-go-driver client",
		loggerEntry:      rkentry.NewLoggerEntryStdout(),
		mongoDbMap:       make(map[string]*mongo.Database),
		mongoDbOpts:      make(map[string][]*mongoOpt.DatabaseOptions),
		pingTimeoutMs:    3 * time.Second,
		Opts:             mongoOpt.Client().ApplyURI("mongodb://localhost:27017"),
	}

	for i := range opts {
		opts[i](entry)
	}

	if len(entry.Opts.Hosts) < 1 {
		entry.Opts.ApplyURI("mongodb://localhost:27017")
	}

	if len(entry.entryName) < 1 {
		entry.entryName = "mongo-" + strings.Join(entry.Opts.Hosts, "-")
	}

	if len(entry.entryDescription) < 1 {
		entry.entryDescription = fmt.Sprintf("%s entry with name of %s",
			entry.entryType,
			entry.entryName)
	}

	rkentry.GlobalAppCtx.AddEntry(entry)

	return entry
}

// MongoEntry will init mongo.Client with provided arguments
type MongoEntry struct {
	entryName        string                                 `yaml:"entryName" yaml:"entryName"`
	entryType        string                                 `yaml:"entryType" yaml:"entryType"`
	entryDescription string                                 `yaml:"-" json:"-"`
	Opts             *mongoOpt.ClientOptions                `yaml:"-" json:"-"`
	Client           *mongo.Client                          `yaml:"-" json:"-"`
	mongoDbMap       map[string]*mongo.Database             `yaml:"-" json:"-"`
	mongoDbOpts      map[string][]*mongoOpt.DatabaseOptions `yaml:"-" json:"-"`
	certEntry        *rkentry.CertEntry                     `yaml:"-" json:"-"`
	loggerEntry      *rkentry.LoggerEntry                   `yaml:"-" json:"-"`
	pingTimeoutMs    time.Duration                          `yaml:"-" json:"-"`
}

// Bootstrap MongoEntry
func (entry *MongoEntry) Bootstrap(ctx context.Context) {
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

	entry.loggerEntry.Info("Bootstrap mongoDbEntry", fields...)

	// connect to mongo
	entry.loggerEntry.Info(fmt.Sprintf("Creating mongoDB client at %v", entry.Opts.Hosts))

	if client, err := mongo.Connect(context.Background(), entry.Opts); err != nil {
		entry.loggerEntry.Error(fmt.Sprintf("Creating mongoDB client at %v failed", entry.Opts.Hosts))
		rkentry.ShutdownWithError(err)
	} else {
		entry.loggerEntry.Info(fmt.Sprintf("Creating mongoDB client at %v success", entry.Opts.Hosts))
		entry.Client = client
	}

	// try ping
	pingCtx, _ := context.WithTimeout(context.Background(), entry.pingTimeoutMs)
	if err := entry.Client.Ping(pingCtx, nil); err != nil {
		entry.loggerEntry.Error(fmt.Sprintf("Ping mongoDB at %v failed", entry.Opts.Hosts))
		rkentry.ShutdownWithError(err)
	}

	// create database
	for k, v := range entry.mongoDbOpts {
		entry.mongoDbMap[k] = entry.Client.Database(k, v...)
		entry.loggerEntry.Info(fmt.Sprintf("Creating database instance [%s] success", k))
	}
}

// Interrupt MongoEntry
func (entry *MongoEntry) Interrupt(ctx context.Context) {
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

	entry.loggerEntry.Info("Interrupt mongoDbEntry", fields...)

	if entry.Client != nil {
		if err := entry.Client.Disconnect(context.Background()); err != nil {
			entry.loggerEntry.Warn(fmt.Sprintf("Disconnecting from mongoDB at %v failed", entry.Opts.Hosts))
		} else {
			entry.loggerEntry.Info(fmt.Sprintf("Disconnecting from mongoDB at %v success", entry.Opts.Hosts))
		}
	}
}

// GetName returns entry name
func (entry *MongoEntry) GetName() string {
	return entry.entryName
}

// GetType returns entry type
func (entry *MongoEntry) GetType() string {
	return entry.entryType
}

// GetDescription returns entry description
func (entry *MongoEntry) GetDescription() string {
	return entry.entryDescription
}

// String returns json marshalled string
func (entry *MongoEntry) String() string {
	bytes, err := json.Marshal(entry)
	if err != nil || len(bytes) < 1 {
		return "{}"
	}

	return string(bytes)
}

// GetMongoClient returns mongo.Client
func (entry *MongoEntry) GetMongoClient() *mongo.Client {
	return entry.Client
}

// GetMongoDB returns mongo.Database
func (entry *MongoEntry) GetMongoDB(dbName string) *mongo.Database {
	return entry.mongoDbMap[dbName]
}

// GetMongoClientOptions returns options.ClientOptions
func (entry *MongoEntry) GetMongoClientOptions() *mongoOpt.ClientOptions {
	return entry.Opts
}

// ************ Option ************

// Option for MongoEntry
type Option func(entry *MongoEntry)

// WithName provide name.
func WithName(name string) Option {
	return func(entry *MongoEntry) {
		entry.entryName = name
	}
}

// WithDescription provide name.
func WithDescription(description string) Option {
	return func(entry *MongoEntry) {
		entry.entryDescription = description
	}
}

// WithCertEntry provide CertEntry
func WithCertEntry(in *rkentry.CertEntry) Option {
	return func(entry *MongoEntry) {
		entry.certEntry = in
	}
}

func WithDatabase(dbName string, dbOpts ...*mongoOpt.DatabaseOptions) Option {
	return func(entry *MongoEntry) {
		if _, ok := entry.mongoDbOpts[dbName]; !ok {
			entry.mongoDbOpts[dbName] = make([]*mongoOpt.DatabaseOptions, 0)
		}

		entry.mongoDbOpts[dbName] = append(entry.mongoDbOpts[dbName], dbOpts...)
	}
}

// WithClientOptions provide options.ClientOptions
func WithClientOptions(opt *mongoOpt.ClientOptions) Option {
	return func(e *MongoEntry) {
		if opt != nil {
			e.Opts = opt
		}
	}
}

// WithLoggerEntry provide rkentry.LoggerEntry entry name
func WithLoggerEntry(entry *rkentry.LoggerEntry) Option {
	return func(m *MongoEntry) {
		if entry != nil {
			m.loggerEntry = entry
		}
	}
}

func WithPingTimeoutMs(tout int) Option {
	return func(entry *MongoEntry) {
		if tout > 0 {
			entry.pingTimeoutMs = time.Duration(tout) * time.Millisecond
		}
	}
}
