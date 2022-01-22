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
	"github.com/rookie-ninja/rk-common/common"
	"github.com/rookie-ninja/rk-entry/entry"
	"github.com/rookie-ninja/rk-logger"
	"go.mongodb.org/mongo-driver/mongo"
	mongoOpt "go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
	"strings"
	"time"
)

// This must be declared in order to register registration function into rk context
// otherwise, rk-boot won't able to bootstrap echo entry automatically from boot config file
func init() {
	rkentry.RegisterEntryRegFunc(RegisterMongoEntriesFromConfig)
}

// GetMongoEntry returns MongoEntry
func GetMongoEntry(entryName string) *MongoEntry {
	if v := rkentry.GlobalAppCtx.GetEntry(entryName); v != nil {
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

// BootConfig
// MongoEntry boot config which reflects to YAML config
type BootConfig struct {
	Mongo []struct {
		Name        string          `yaml:"name" json:"name"`
		Enabled     bool            `yaml:"enabled" json:"enabled"`
		Description string          `yaml:"description" json:"description"`
		SimpleURI   string          `yaml:"simpleURI" json:"simpleURI"`
		Base        BootConfigMongo `mapstructure:",squash"`
		Database    []struct {
			Name string `yaml:"name" json:"name"`
		}
		Logger struct {
			Encoding    string   `yaml:"encoding" json:"encoding"`
			OutputPaths []string `yaml:"outputPaths" json:"outputPaths"`
		} `yaml:"logger" json:"logger"`
		CertEntry string `yaml:"certEntry" json:"certEntry"`
	} `yaml:"mongo" json:"mongo"`
}

// BootConfigMongo sub struct for BootConfig
type BootConfigMongo struct {
	AppName *string `yaml:"appName" json:"appName"`
	Auth    *struct {
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
func ToClientOptions(config *BootConfigMongo) *mongoOpt.ClientOptions {
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

	return opt
}

// RegisterMongoEntriesFromConfig register MongoEntry based on config file into rkentry.GlobalAppCtx
func RegisterMongoEntriesFromConfig(configFilePath string) map[string]rkentry.Entry {
	res := make(map[string]rkentry.Entry)

	// 1: unmarshal user provided config into boot config struct
	config := &BootConfig{}
	rkcommon.UnmarshalBootConfig(configFilePath, config)

	for i := range config.Mongo {
		element := config.Mongo[i]

		if element.Enabled {
			clientOpt := ToClientOptions(&element.Base)

			certEntry := rkentry.GlobalAppCtx.GetCertEntry(element.CertEntry)

			opts := []Option{
				WithName(element.Name),
				WithDescription(element.Description),
				WithClientOptions(clientOpt),
				WithCertEntry(certEntry),
				WithLoggerEncoding(element.Logger.Encoding),
				WithLoggerOutputPaths(element.Logger.OutputPaths...),
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
		EntryName:        "Mongo",
		EntryType:        "Mongo",
		EntryDescription: "Mongo entry for mongo-go-driver client",
		loggerOutputPath: make([]string, 0),
		mongoDbMap:       make(map[string]*mongo.Database),
		mongoDbOpts:      make(map[string][]*mongoOpt.DatabaseOptions),
		loggerEncoding:   rklogger.EncodingConsole,
		Opts:             mongoOpt.Client().ApplyURI("mongodb://localhost:27017"),
	}

	for i := range opts {
		opts[i](entry)
	}

	if len(entry.Opts.Hosts) < 1 {
		entry.Opts.ApplyURI("mongodb://localhost:27017")
	}

	if len(entry.EntryName) < 1 {
		entry.EntryName = "mongo-" + strings.Join(entry.Opts.Hosts, "-")
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

	rkentry.GlobalAppCtx.AddEntry(entry)

	return entry
}

// MongoEntry will init mongo.Client with provided arguments
type MongoEntry struct {
	EntryName        string                                 `yaml:"entryName" yaml:"entryName"`
	EntryType        string                                 `yaml:"entryType" yaml:"entryType"`
	EntryDescription string                                 `yaml:"-" json:"-"`
	Opts             *mongoOpt.ClientOptions                `yaml:"-" json:"-"`
	Client           *mongo.Client                          `yaml:"-" json:"-"`
	mongoDbMap       map[string]*mongo.Database             `yaml:"-" json:"-"`
	mongoDbOpts      map[string][]*mongoOpt.DatabaseOptions `yaml:"-" json:"-"`
	Logger           *zap.Logger                            `yaml:"-" json:"-"`
	certEntry        *rkentry.CertEntry                     `yaml:"-" json:"-"`
	loggerEncoding   string                                 `yaml:"-" json:"-"`
	loggerOutputPath []string                               `yaml:"-" json:"-"`
}

// Bootstrap MongoEntry
func (entry *MongoEntry) Bootstrap(ctx context.Context) {
	entry.Logger.Info("Bootstrap mongoDB entry",
		zap.String("entryName", entry.EntryName))

	// connect to mongo
	entry.Logger.Info(fmt.Sprintf("Creating mongoDB client at %v", entry.Opts.Hosts))

	if client, err := mongo.Connect(context.Background(), entry.Opts); err != nil {
		entry.Logger.Info(fmt.Sprintf("Creating mongoDB client at %v failed", entry.Opts.Hosts))
		rkcommon.ShutdownWithError(err)
	} else {
		entry.Logger.Info(fmt.Sprintf("Creating mongoDB client at %v success", entry.Opts.Hosts))
		entry.Client = client
	}

	// create database
	for k, v := range entry.mongoDbOpts {
		entry.mongoDbMap[k] = entry.Client.Database(k, v...)
		entry.Logger.Info(fmt.Sprintf("Creating database instance [%s] success", k))
	}
}

// Interrupt MongoEntry
func (entry *MongoEntry) Interrupt(ctx context.Context) {
	entry.Logger.Info("Interrupt mongoDB entry",
		zap.String("entryName", entry.EntryName))

	if entry.Client != nil {
		if err := entry.Client.Disconnect(context.Background()); err != nil {
			entry.Logger.Info(fmt.Sprintf("Disconnecting from mongoDB at %v failed", entry.Opts.Hosts))
		} else {
			entry.Logger.Info(fmt.Sprintf("Disconnecting from mongoDB at %v success", entry.Opts.Hosts))
		}
	}
}

// GetName returns entry name
func (entry *MongoEntry) GetName() string {
	return entry.EntryName
}

// GetType returns entry type
func (entry *MongoEntry) GetType() string {
	return entry.EntryType
}

// GetDescription returns entry description
func (entry *MongoEntry) GetDescription() string {
	return entry.EntryDescription
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
		entry.EntryName = name
	}
}

// WithDescription provide name.
func WithDescription(description string) Option {
	return func(entry *MongoEntry) {
		entry.EntryDescription = description
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

// WithLoggerEncoding provide console=0, json=1.
// json or console is supported.
func WithLoggerEncoding(ec string) Option {
	return func(m *MongoEntry) {
		m.loggerEncoding = strings.ToLower(ec)
	}
}

// WithLoggerOutputPaths provide Logger Output Path.
// Multiple output path could be supported including stdout.
func WithLoggerOutputPaths(path ...string) Option {
	return func(m *MongoEntry) {
		m.loggerOutputPath = append(m.loggerOutputPath, path...)
	}
}
