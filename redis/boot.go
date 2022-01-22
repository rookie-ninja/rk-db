// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.

// Package rkredis is an implementation of rkentry.Entry which could be used redis client instance.
package rkredis

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/rookie-ninja/rk-common/common"
	"github.com/rookie-ninja/rk-entry/entry"
	"github.com/rookie-ninja/rk-logger"
	"go.uber.org/zap"
	"strings"
	"time"
)

// This must be declared in order to register registration function into rk context
// otherwise, rk-boot won't able to bootstrap echo entry automatically from boot config file
func init() {
	rkentry.RegisterEntryRegFunc(RegisterRedisEntryFromConfig)
}

const (
	ha      = "HA"
	cluster = "Cluster"
	single  = "Single"
)

// GetRedisEntry returns RedisEntry
func GetRedisEntry(entryName string) *RedisEntry {
	if v := rkentry.GlobalAppCtx.GetEntry(entryName); v != nil {
		if res, ok := v.(*RedisEntry); ok {
			return res
		}
	}

	return nil
}

// BootConfig
// Redis entry boot config which reflects to YAML config
type BootConfig struct {
	Redis []struct {
		BootConfigRedis `mapstructure:",squash"`
	} `yaml:"redis" json:"redis"`
}

// BootConfigRedis sub struct for BootConfig
type BootConfigRedis struct {
	Name                 string   `yaml:"name" json:"name"` // Required
	Description          string   `yaml:"description" json:"description"`
	Enabled              bool     `yaml:"enabled" json:"enabled"` // Required
	Addrs                []string `yaml:"addrs" json:"addrs"`     // Required
	MasterName           string   `yaml:"masterName" json:"masterName"`
	SentinelPass         string   `yaml:"sentinelPass" json:"sentinelPass"`
	DB                   int      `yaml:"db" json:"db"`     // Required
	User                 string   `yaml:"user" json:"user"` // Required
	Pass                 string   `yaml:"pass" json:"pass"` // Required
	MaxRetries           int      `yaml:"maxRetries" json:"maxRetries"`
	MinRetryBackoffMs    int      `yaml:"minRetryBackoffMs" json:"minRetryBackoffMs"`
	MaxRetryBackoffMs    int      `yaml:"maxRetryBackoffMs" json:"maxRetryBackoffMs"`
	DialTimeoutMs        int      `yaml:"dialTimeoutMs" json:"dialTimeoutMs"`
	ReadTimeoutMs        int      `yaml:"readTimeoutMs" json:"readTimeoutMs"`
	WriteTimeoutMs       int      `yaml:"writeTimeoutMs" json:"writeTimeoutMs"`
	PoolFIFO             bool     `yaml:"poolFIFO" json:"poolFIFO"`
	PoolSize             int      `yaml:"poolSize" json:"poolSize"`
	MinIdleConn          int      `yaml:"minIdleConn" json:"minIdleConn"`
	MaxConnAgeMs         int      `yaml:"maxConnAgeMs" json:"maxConnAgeMs"`
	PoolTimeoutMs        int      `yaml:"poolTimeoutMs" json:"poolTimeoutMs"`
	IdleTimeoutMs        int      `yaml:"idleTimeoutMs" json:"idleTimeoutMs"`
	IdleCheckFrequencyMs int      `yaml:"idleCheckFrequencyMs" json:"idleCheckFrequencyMs"`
	MaxRedirects         int      `yaml:"maxRedirects" json:"maxRedirects"`
	ReadOnly             bool     `yaml:"readOnly" json:"readOnly"`
	RouteByLatency       bool     `yaml:"routeByLatency" json:"routeByLatency"`
	RouteRandomly        bool     `yaml:"routeRandomly" json:"routeRandomly"`
	Logger               struct {
		Encoding    string   `yaml:"encoding" json:"encoding"`
		OutputPaths []string `yaml:"outputPaths" json:"outputPaths"`
	} `yaml:"logger" json:"logger"`
	Cert struct {
		Ref string `yaml:"ref" json:"ref"`
	} `yaml:"cert" json:"cert"`
}

// ToRedisUniversalOptions convert BootConfigRedis to redis.UniversalOptions
func ToRedisUniversalOptions(config *BootConfigRedis) *redis.UniversalOptions {
	if config.Enabled {
		return &redis.UniversalOptions{
			Addrs:              config.Addrs,
			DB:                 config.DB,
			Username:           config.User,
			Password:           config.Pass,
			SentinelPassword:   config.SentinelPass,
			MaxRetries:         config.MaxRetries,
			MinRetryBackoff:    time.Duration(config.MinRetryBackoffMs) * time.Millisecond,
			MaxRetryBackoff:    time.Duration(config.MaxRetryBackoffMs) * time.Millisecond,
			DialTimeout:        time.Duration(config.DialTimeoutMs) * time.Millisecond,
			ReadTimeout:        time.Duration(config.ReadTimeoutMs) * time.Millisecond,
			WriteTimeout:       time.Duration(config.WriteTimeoutMs) * time.Millisecond,
			PoolFIFO:           config.PoolFIFO,
			PoolSize:           config.PoolSize,
			MinIdleConns:       config.MinIdleConn,
			MaxConnAge:         time.Duration(config.MaxConnAgeMs) * time.Millisecond,
			PoolTimeout:        time.Duration(config.PoolTimeoutMs) * time.Millisecond,
			IdleTimeout:        time.Duration(config.IdleTimeoutMs) * time.Millisecond,
			IdleCheckFrequency: time.Duration(config.IdleCheckFrequencyMs) * time.Millisecond,
			MaxRedirects:       config.MaxRedirects,
			ReadOnly:           config.ReadOnly,
			RouteByLatency:     config.RouteByLatency,
			RouteRandomly:      config.RouteRandomly,
			MasterName:         config.MasterName,
		}
	} else {
		return nil
	}
}

// RegisterRedisEntryFromConfig register RedisEntry based on config file into rkentry.GlobalAppCtx
func RegisterRedisEntryFromConfig(configFilePath string) map[string]rkentry.Entry {
	res := make(map[string]rkentry.Entry)

	// 1: unmarshal user provided config into boot config struct
	config := &BootConfig{}
	rkcommon.UnmarshalBootConfig(configFilePath, config)

	for i := range config.Redis {
		element := config.Redis[i]

		if element.Enabled {
			universalOpt := &redis.UniversalOptions{
				Addrs:              element.Addrs,
				DB:                 element.DB,
				Username:           element.User,
				Password:           element.Pass,
				SentinelPassword:   element.SentinelPass,
				MaxRetries:         element.MaxRetries,
				MinRetryBackoff:    time.Duration(element.MinRetryBackoffMs) * time.Millisecond,
				MaxRetryBackoff:    time.Duration(element.MaxRetryBackoffMs) * time.Millisecond,
				DialTimeout:        time.Duration(element.DialTimeoutMs) * time.Millisecond,
				ReadTimeout:        time.Duration(element.ReadTimeoutMs) * time.Millisecond,
				WriteTimeout:       time.Duration(element.WriteTimeoutMs) * time.Millisecond,
				PoolFIFO:           element.PoolFIFO,
				PoolSize:           element.PoolSize,
				MinIdleConns:       element.MinIdleConn,
				MaxConnAge:         time.Duration(element.MaxConnAgeMs) * time.Millisecond,
				PoolTimeout:        time.Duration(element.PoolTimeoutMs) * time.Millisecond,
				IdleTimeout:        time.Duration(element.IdleTimeoutMs) * time.Millisecond,
				IdleCheckFrequency: time.Duration(element.IdleCheckFrequencyMs) * time.Millisecond,
				MaxRedirects:       element.MaxRedirects,
				ReadOnly:           element.ReadOnly,
				RouteByLatency:     element.RouteByLatency,
				RouteRandomly:      element.RouteRandomly,
				MasterName:         element.MasterName,
			}

			certEntry := rkentry.GlobalAppCtx.GetCertEntry(element.Cert.Ref)

			entry := RegisterRedisEntry(
				WithName(element.Name),
				WithDescription(element.Description),
				WithUniversalOption(universalOpt),
				WithCertEntry(certEntry),
				WithLoggerEncoding(element.Logger.Encoding),
				WithLoggerOutputPaths(element.Logger.OutputPaths...))

			res[entry.GetName()] = entry
		}
	}

	return res
}

// RegisterRedisEntry will register Entry into GlobalAppCtx
func RegisterRedisEntry(opts ...Option) *RedisEntry {
	entry := &RedisEntry{
		EntryName:        "Redis",
		EntryType:        "Redis",
		EntryDescription: "Redis entry for go-redis client",
		loggerOutputPath: make([]string, 0),
		loggerEncoding:   rklogger.EncodingConsole,
		Opts: &redis.UniversalOptions{
			Addrs: []string{"localhost:6379"},
		},
	}

	for i := range opts {
		opts[i](entry)
	}

	if len(entry.Opts.Addrs) < 1 {
		entry.Opts.Addrs = append(entry.Opts.Addrs, "localhost:6379")
	}

	if len(entry.EntryName) < 1 {
		entry.EntryName = "redis-" + strings.Join(entry.Opts.Addrs, "-")
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

	redis.SetLogger(NewLogger(entry.Logger))

	rkentry.GlobalAppCtx.AddEntry(entry)

	return entry
}

// RedisEntry will init redis.Client with provided arguments
type RedisEntry struct {
	EntryName        string                  `yaml:"entryName" yaml:"entryName"`
	EntryType        string                  `yaml:"entryType" yaml:"entryType"`
	EntryDescription string                  `yaml:"-" json:"-"`
	ClientType       string                  `yaml:"clientType" json:"clientType"`
	Opts             *redis.UniversalOptions `yaml:"-" json:"-"`
	certEntry        *rkentry.CertEntry      `yaml:"-" json:"-"`
	loggerEncoding   string                  `yaml:"-" json:"-"`
	loggerOutputPath []string                `yaml:"-" json:"-"`
	Logger           *zap.Logger             `yaml:"-" json:"-"`
	Client           redis.UniversalClient   `yaml:"-" json:"-"`
}

// Bootstrap RedisEntry
func (entry *RedisEntry) Bootstrap(ctx context.Context) {
	if entry.Opts.MasterName != "" {
		entry.ClientType = ha
	} else if len(entry.Opts.Addrs) > 1 {
		entry.ClientType = cluster
	} else {
		entry.ClientType = single
	}

	entry.Logger.Info("Bootstrap redis entry",
		zap.String("entryName", entry.EntryName),
		zap.String("clientType", entry.ClientType))

	if entry.IsTlsEnabled() {
		if cert, err := tls.X509KeyPair(entry.certEntry.Store.ServerCert, entry.certEntry.Store.ServerKey); err != nil {
			entry.Logger.Error("Error occurs while parsing TLS.")
			rkcommon.ShutdownWithError(err)
		} else {
			entry.Opts.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
		}
	}

	entry.Client = redis.NewUniversalClient(entry.Opts)

	if entry.Client != nil {
		entry.Client.AddHook(NewRedisTracer())
	}
}

// Interrupt RedisEntry
func (entry *RedisEntry) Interrupt(ctx context.Context) {
	entry.Logger.Info("Interrupt redis entry",
		zap.String("entryName", entry.EntryName),
		zap.String("clientType", entry.ClientType))

	rkentry.GlobalAppCtx.RemoveEntry(entry.GetName())
}

// GetName returns entry name
func (entry *RedisEntry) GetName() string {
	return entry.EntryName
}

// GetType returns entry type
func (entry *RedisEntry) GetType() string {
	return entry.EntryType
}

// GetDescription returns entry description
func (entry *RedisEntry) GetDescription() string {
	return entry.EntryDescription
}

// String returns json marshalled string
func (entry *RedisEntry) String() string {
	bytes, err := json.Marshal(entry)
	if err != nil || len(bytes) < 1 {
		return "{}"
	}

	return string(bytes)
}

// IsTlsEnabled checks TLS
func (entry *RedisEntry) IsTlsEnabled() bool {
	return entry.certEntry != nil && entry.certEntry.Store != nil
}

// GetClient convert redis.UniversalClient to proper redis.Client
func (entry *RedisEntry) GetClient() (*redis.Client, bool) {
	if entry.Client != nil && (entry.ClientType == ha || entry.ClientType == single) {
		if v, ok := entry.Client.(*redis.Client); ok {
			return v, true
		}
	}

	return nil, false
}

// GetClient convert redis.UniversalClient to proper redis.ClusterClient
func (entry *RedisEntry) GetClientCluster() (*redis.ClusterClient, bool) {
	if entry.Client != nil && entry.ClientType == cluster {
		if v, ok := entry.Client.(*redis.ClusterClient); ok {
			return v, true
		}
	}

	return nil, false
}

// ************* Option *************

// Option for RedisEntry
type Option func(e *RedisEntry)

// WithName provide name.
func WithName(name string) Option {
	return func(entry *RedisEntry) {
		entry.EntryName = name
	}
}

// WithDescription provide name.
func WithDescription(description string) Option {
	return func(entry *RedisEntry) {
		entry.EntryDescription = description
	}
}

// WithCertEntry provide CertEntry
func WithCertEntry(in *rkentry.CertEntry) Option {
	return func(entry *RedisEntry) {
		entry.certEntry = in
	}
}

// WithUniversalOption provide redis.UniversalOptions
func WithUniversalOption(opt *redis.UniversalOptions) Option {
	return func(e *RedisEntry) {
		if opt != nil {
			e.Opts = opt
		}
	}
}

// WithLoggerEncoding provide console=0, json=1.
// json or console is supported.
func WithLoggerEncoding(ec string) Option {
	return func(m *RedisEntry) {
		m.loggerEncoding = strings.ToLower(ec)
	}
}

// WithLoggerOutputPaths provide Logger Output Path.
// Multiple output path could be supported including stdout.
func WithLoggerOutputPaths(path ...string) Option {
	return func(m *RedisEntry) {
		m.loggerOutputPath = append(m.loggerOutputPath, path...)
	}
}