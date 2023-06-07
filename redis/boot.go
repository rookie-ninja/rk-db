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
	"github.com/redis/go-redis/v9"
	"github.com/rookie-ninja/rk-entry/v2/entry"
	"go.uber.org/zap"
	"strings"
	"time"
)

// This must be declared in order to register registration function into rk context
// otherwise, rk-boot won't able to bootstrap echo entry automatically from boot config file
func init() {
	rkentry.RegisterPluginRegFunc(RegisterRedisEntryYAML)
}

const (
	ha      = "HA"
	cluster = "Cluster"
	single  = "Single"

	RedisEntryType = "RedisEntry"
)

// GetRedisEntry returns RedisEntry
func GetRedisEntry(entryName string) *RedisEntry {
	if v := rkentry.GlobalAppCtx.GetEntry(RedisEntryType, entryName); v != nil {
		if res, ok := v.(*RedisEntry); ok {
			return res
		}
	}

	return nil
}

// BootRedis
// Redis entry boot config which reflects to YAML config
type BootRedis struct {
	Redis []*BootRedisE `yaml:"redis" json:"redis"`
}

// BootRedisE sub struct for BootRedis
type BootRedisE struct {
	Name                  string   `yaml:"name" json:"name"` // Required
	Description           string   `yaml:"description" json:"description"`
	Enabled               bool     `yaml:"enabled" json:"enabled"` // Required
	Domain                string   `yaml:"domain" json:"domain"`
	Addrs                 []string `yaml:"addrs" json:"addrs"` // Required
	MasterName            string   `yaml:"masterName" json:"masterName"`
	SentinelPass          string   `yaml:"sentinelPass" json:"sentinelPass"`
	DB                    int      `yaml:"db" json:"db"`     // Required
	User                  string   `yaml:"user" json:"user"` // Required
	Pass                  string   `yaml:"pass" json:"pass"` // Required
	MaxRetries            int      `yaml:"maxRetries" json:"maxRetries"`
	MinRetryBackoffMs     int      `yaml:"minRetryBackoffMs" json:"minRetryBackoffMs"`
	MaxRetryBackoffMs     int      `yaml:"maxRetryBackoffMs" json:"maxRetryBackoffMs"`
	DialTimeoutMs         int      `yaml:"dialTimeoutMs" json:"dialTimeoutMs"`
	ReadTimeoutMs         int      `yaml:"readTimeoutMs" json:"readTimeoutMs"`
	WriteTimeoutMs        int      `yaml:"writeTimeoutMs" json:"writeTimeoutMs"`
	ContextTimeoutEnabled bool     `yaml:"contextTimeoutEnabled" json:"contextTimeoutEnabled"`
	PoolFIFO              bool     `yaml:"poolFIFO" json:"poolFIFO"`
	PoolSize              int      `yaml:"poolSize" json:"poolSize"`
	MinIdleConn           int      `yaml:"minIdleConn" json:"minIdleConn"`
	MaxIdleConn           int      `yaml:"maxIdleConn" json:"maxIdleConn"`
	ConnMaxIdleTimeMs     int      `yaml:"connMaxIdleTimeMs" json:"connMaxIdleTimeMs"`
	ConnMaxLifetimeMs     int      `yaml:"connMaxLifetimeMs" json:"connMaxLifetimeMs"`
	PoolTimeoutMs         int      `yaml:"poolTimeoutMs" json:"poolTimeoutMs"`
	IdleTimeoutMs         int      `yaml:"idleTimeoutMs" json:"idleTimeoutMs"`
	IdleCheckFrequencyMs  int      `yaml:"idleCheckFrequencyMs" json:"idleCheckFrequencyMs"`
	MaxRedirects          int      `yaml:"maxRedirects" json:"maxRedirects"`
	ReadOnly              bool     `yaml:"readOnly" json:"readOnly"`
	RouteByLatency        bool     `yaml:"routeByLatency" json:"routeByLatency"`
	RouteRandomly         bool     `yaml:"routeRandomly" json:"routeRandomly"`
	LoggerEntry           string   `yaml:"loggerEntry" json:"loggerEntry"`
	CertEntry             string   `yaml:"certEntry" json:"certEntry"`
}

// ToRedisUniversalOptions convert BootConfigRedis to redis.UniversalOptions
func ToRedisUniversalOptions(config *BootRedisE) *redis.UniversalOptions {
	if config.Enabled {
		return &redis.UniversalOptions{
			Addrs:                 config.Addrs,
			DB:                    config.DB,
			Username:              config.User,
			Password:              config.Pass,
			SentinelPassword:      config.SentinelPass,
			MaxRetries:            config.MaxRetries,
			MinRetryBackoff:       time.Duration(config.MinRetryBackoffMs) * time.Millisecond,
			MaxRetryBackoff:       time.Duration(config.MaxRetryBackoffMs) * time.Millisecond,
			DialTimeout:           time.Duration(config.DialTimeoutMs) * time.Millisecond,
			ReadTimeout:           time.Duration(config.ReadTimeoutMs) * time.Millisecond,
			WriteTimeout:          time.Duration(config.WriteTimeoutMs) * time.Millisecond,
			ContextTimeoutEnabled: config.ContextTimeoutEnabled,

			PoolFIFO:     config.PoolFIFO,
			PoolSize:     config.PoolSize,
			PoolTimeout:  time.Duration(config.PoolTimeoutMs) * time.Millisecond,
			MinIdleConns: config.MinIdleConn,
			MaxIdleConns: config.MaxIdleConn,

			ConnMaxIdleTime: time.Duration(config.ConnMaxIdleTimeMs) * time.Millisecond,
			ConnMaxLifetime: time.Duration(config.ConnMaxLifetimeMs) * time.Millisecond,

			MaxRedirects:   config.MaxRedirects,
			ReadOnly:       config.ReadOnly,
			RouteByLatency: config.RouteByLatency,
			RouteRandomly:  config.RouteRandomly,
			MasterName:     config.MasterName,
		}
	} else {
		return nil
	}
}

// RegisterRedisEntryYAML register RedisEntry based on config file into rkentry.GlobalAppCtx
func RegisterRedisEntryYAML(raw []byte) map[string]rkentry.Entry {
	res := make(map[string]rkentry.Entry)

	// 1: unmarshal user provided config into boot config struct
	config := &BootRedis{}
	rkentry.UnmarshalBootYAML(raw, config)

	// filter out based domain
	configMap := make(map[string]*BootRedisE)
	for _, e := range config.Redis {
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
		universalOpt := &redis.UniversalOptions{
			Addrs:                 element.Addrs,
			DB:                    element.DB,
			Username:              element.User,
			Password:              element.Pass,
			SentinelPassword:      element.SentinelPass,
			MaxRetries:            element.MaxRetries,
			MinRetryBackoff:       time.Duration(element.MinRetryBackoffMs) * time.Millisecond,
			MaxRetryBackoff:       time.Duration(element.MaxRetryBackoffMs) * time.Millisecond,
			DialTimeout:           time.Duration(element.DialTimeoutMs) * time.Millisecond,
			ReadTimeout:           time.Duration(element.ReadTimeoutMs) * time.Millisecond,
			WriteTimeout:          time.Duration(element.WriteTimeoutMs) * time.Millisecond,
			ContextTimeoutEnabled: element.ContextTimeoutEnabled,

			PoolFIFO:     element.PoolFIFO,
			PoolSize:     element.PoolSize,
			PoolTimeout:  time.Duration(element.PoolTimeoutMs) * time.Millisecond,
			MinIdleConns: element.MinIdleConn,
			MaxIdleConns: element.MaxIdleConn,

			ConnMaxIdleTime: time.Duration(element.ConnMaxIdleTimeMs) * time.Millisecond,
			ConnMaxLifetime: time.Duration(element.ConnMaxLifetimeMs) * time.Millisecond,

			MaxRedirects:   element.MaxRedirects,
			ReadOnly:       element.ReadOnly,
			RouteByLatency: element.RouteByLatency,
			RouteRandomly:  element.RouteRandomly,
			MasterName:     element.MasterName,
		}

		certEntry := rkentry.GlobalAppCtx.GetCertEntry(element.CertEntry)

		entry := RegisterRedisEntry(
			WithName(element.Name),
			WithDescription(element.Description),
			WithUniversalOption(universalOpt),
			WithCertEntry(certEntry),
			WithLoggerEntry(rkentry.GlobalAppCtx.GetLoggerEntry(element.LoggerEntry)))

		res[entry.GetName()] = entry
	}

	return res
}

// RegisterRedisEntry will register Entry into GlobalAppCtx
func RegisterRedisEntry(opts ...Option) *RedisEntry {
	entry := &RedisEntry{
		entryName:        "Redis",
		entryType:        RedisEntryType,
		entryDescription: "Redis entry for go-redis client",
		loggerEntry:      rkentry.GlobalAppCtx.GetLoggerEntryDefault(),
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

	if len(entry.entryName) < 1 {
		entry.entryName = "redis-" + strings.Join(entry.Opts.Addrs, "-")
	}

	if len(entry.entryDescription) < 1 {
		entry.entryDescription = fmt.Sprintf("%s entry with name of %s",
			entry.entryType,
			entry.entryName)
	}

	redis.SetLogger(NewLogger(entry.loggerEntry.Logger))

	rkentry.GlobalAppCtx.AddEntry(entry)

	return entry
}

// RedisEntry will init redis.Client with provided arguments
type RedisEntry struct {
	entryName        string                  `yaml:"entryName" yaml:"entryName"`
	entryType        string                  `yaml:"entryType" yaml:"entryType"`
	entryDescription string                  `yaml:"-" json:"-"`
	ClientType       string                  `yaml:"clientType" json:"clientType"`
	Opts             *redis.UniversalOptions `yaml:"-" json:"-"`
	certEntry        *rkentry.CertEntry      `yaml:"-" json:"-"`
	loggerEntry      *rkentry.LoggerEntry    `yaml:"-" json:"-"`
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

	// extract eventId if exists
	fields := make([]zap.Field, 0)

	if val := ctx.Value("eventId"); val != nil {
		if id, ok := val.(string); ok {
			fields = append(fields, zap.String("eventId", id))
		}
	}

	fields = append(fields,
		zap.String("entryName", entry.entryName),
		zap.String("entryType", entry.entryType),
		zap.String("clientType", entry.ClientType))

	entry.loggerEntry.Info("Bootstrap RedisEntry", fields...)

	if entry.IsTlsEnabled() {
		entry.Opts.TLSConfig = &tls.Config{Certificates: []tls.Certificate{*entry.certEntry.Certificate}}
	}

	entry.Client = redis.NewUniversalClient(entry.Opts)

	entry.loggerEntry.Info(fmt.Sprintf("Ping redis at %s", entry.Opts.Addrs))
	cmd := entry.Client.Ping(context.Background())
	if cmd.Err() != nil {
		entry.loggerEntry.Info(fmt.Sprintf("Ping redis at %s failed", entry.Opts.Addrs))
		rkentry.ShutdownWithError(cmd.Err())
	}
	entry.loggerEntry.Info(fmt.Sprintf("Ping redis at %s success", entry.Opts.Addrs))

	if entry.Client != nil {
		entry.Client.AddHook(NewRedisTracer())
	}
}

// Interrupt RedisEntry
func (entry *RedisEntry) Interrupt(ctx context.Context) {
	// extract eventId if exists
	fields := make([]zap.Field, 0)

	if val := ctx.Value("eventId"); val != nil {
		if id, ok := val.(string); ok {
			fields = append(fields, zap.String("eventId", id))
		}
	}

	fields = append(fields,
		zap.String("entryName", entry.entryName),
		zap.String("entryType", entry.entryType),
		zap.String("clientType", entry.ClientType))

	entry.loggerEntry.Info("Interrupt RedisEntry", fields...)
}

// GetName returns entry name
func (entry *RedisEntry) GetName() string {
	return entry.entryName
}

// GetType returns entry type
func (entry *RedisEntry) GetType() string {
	return entry.entryType
}

// GetDescription returns entry description
func (entry *RedisEntry) GetDescription() string {
	return entry.entryDescription
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
	return entry.certEntry != nil && entry.certEntry.Certificate != nil
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
		entry.entryName = name
	}
}

// WithDescription provide name.
func WithDescription(description string) Option {
	return func(entry *RedisEntry) {
		entry.entryDescription = description
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

// WithLoggerEntry provide rkentry.LoggerEntry entry name
func WithLoggerEntry(entry *rkentry.LoggerEntry) Option {
	return func(m *RedisEntry) {
		if entry != nil {
			m.loggerEntry = entry
		} else {
			m.loggerEntry = rkentry.GlobalAppCtx.GetLoggerEntryDefault()
		}
	}
}
