package rkredis

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/rookie-ninja/rk-common/common"
	"github.com/rookie-ninja/rk-entry/entry"
	"github.com/rookie-ninja/rk-logger"
	"go.uber.org/zap"
	"os"
	"path"
	"strings"
	"time"
)

// This must be declared in order to register registration function into rk context
// otherwise, rk-boot won't able to bootstrap echo entry automatically from boot config file
func init() {
	rkentry.RegisterEntryRegFunc(RegisterRedisEntryFromConfig)
}

const (
	// EncodingConsole console encoding style of logging
	EncodingConsole = "console"
	// EncodingJson console encoding style of logging
	EncodingJson = "json"

	ha      = "HA"
	cluster = "Cluster"
	single  = "Single"
)

type BootConfig struct {
	Redis []BootConfigRedis `mapstructure:",squash"`
}

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
	}
	Cert struct {
		Ref string `yaml:"ref" json:"ref"`
	} `yaml:"cert" json:"cert"`
}

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

func RegisterRedisEntry(opts ...Option) *RedisEntry {
	entry := &RedisEntry{
		EntryName:        "Redis",
		EntryType:        "Redis",
		EntryDescription: "Redis entry for go-redis client",
		loggerOutputPath: make([]string, 0),
		loggerEncoding:   EncodingConsole,
		Opts: &redis.UniversalOptions{
			Addrs: []string{"localhost:6379"},
		},
	}

	for i := range opts {
		opts[i](entry)
	}

	if len(entry.EntryName) < 1 {
		entry.EntryName = "redis-" + strings.Join(entry.Opts.Addrs, "-")
	}

	if len(entry.EntryDescription) < 1 {
		entry.EntryDescription = fmt.Sprintf("%s entry with name of %s, addr:%s, user:%s",
			entry.EntryType,
			entry.EntryName)
	}

	// Override zap logger encoding and output path if provided by user
	// Override encoding type
	zapLoggerConfig := rklogger.NewZapStdoutConfig()
	lumberjackConfig := rklogger.NewLumberjackConfigDefault()
	if entry.loggerEncoding == EncodingJson || len(entry.loggerOutputPath) > 0 {
		if entry.loggerEncoding == EncodingJson {
			zapLoggerConfig.Encoding = "json"
		}

		if len(entry.loggerOutputPath) > 0 {
			zapLoggerConfig.OutputPaths = toAbsPath(entry.loggerOutputPath...)
		}

		if lumberjackConfig == nil {
			lumberjackConfig = rklogger.NewLumberjackConfigDefault()
		}
	}

	if logger, err := rklogger.NewZapLoggerWithConf(zapLoggerConfig, lumberjackConfig, zap.AddCaller()); err != nil {
		rkcommon.ShutdownWithError(err)
	} else {
		entry.Logger = logger
	}

	redis.SetLogger(NewLogger(entry.Logger))

	rkentry.GlobalAppCtx.AddEntry(entry)

	return entry
}

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

func WithCertEntry(in *rkentry.CertEntry) Option {
	return func(entry *RedisEntry) {
		entry.CertEntry = in
	}
}

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

type RedisEntry struct {
	EntryName        string                  `yaml:"entryName" yaml:"entryName"`
	EntryType        string                  `yaml:"entryType" yaml:"entryType"`
	EntryDescription string                  `yaml:"-" json:"-"`
	ClientType       string                  `yaml:"clientType" json:"clientType"`
	Opts             *redis.UniversalOptions `yaml:"-" json:"-"`
	CertEntry        *rkentry.CertEntry      `yaml:"-" json:"-"`
	loggerEncoding   string                  `yaml:"-" json:"-"`
	loggerOutputPath []string                `yaml:"-" json:"-"`
	Logger           *zap.Logger             `yaml:"-" json:"-"`
	Client           redis.UniversalClient   `yaml:"-" json:"-"`
}

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

	entry.Client = redis.NewUniversalClient(entry.Opts)

	if entry.Client != nil {
		entry.Client.AddHook(NewRedisTracer())
	}
}

func (entry *RedisEntry) Interrupt(ctx context.Context) {
	entry.Logger.Info("Interrupt redis entry",
		zap.String("entryName", entry.EntryName),
		zap.String("clientType", entry.ClientType))

	entry.Client.ShutdownSave(ctx)
}

func (entry *RedisEntry) GetName() string {
	return entry.EntryName
}

func (entry *RedisEntry) GetType() string {
	return entry.EntryType
}

func (entry *RedisEntry) GetDescription() string {
	return entry.EntryDescription
}

func (entry *RedisEntry) String() string {
	bytes, err := json.Marshal(entry)
	if err != nil || len(bytes) < 1 {
		return "{}"
	}

	return string(bytes)
}

func (entry *RedisEntry) GetClient() (*redis.Client, bool) {
	if entry.Client != nil && (entry.ClientType == ha || entry.ClientType == single) {
		if v, ok := entry.Client.(*redis.Client); ok {
			return v, true
		}
	}

	return nil, false
}

func (entry *RedisEntry) GetClientCluster() (*redis.ClusterClient, bool) {
	if entry.Client != nil && entry.ClientType == cluster {
		if v, ok := entry.Client.(*redis.ClusterClient); ok {
			return v, true
		}
	}

	return nil, false
}

// Make incoming paths to absolute path with current working directory attached as prefix
func toAbsPath(p ...string) []string {
	res := make([]string, 0)

	for i := range p {
		newPath := p[i]
		if !path.IsAbs(p[i]) {
			wd, _ := os.Getwd()
			newPath = path.Join(wd, p[i])
		}
		res = append(res, newPath)
	}

	return res
}
