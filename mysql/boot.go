package rkmysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rookie-ninja/rk-common/common"
	"github.com/rookie-ninja/rk-entry/entry"
	"github.com/rookie-ninja/rk-logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapgrpc"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"path"
	"strings"
	"time"
)

const (
	// EncodingConsole console encoding style of logging
	EncodingConsole int = 0
	// EncodingJson console encoding style of logging
	EncodingJson int = 1
)

// This must be declared in order to register registration function into rk context
// otherwise, rk-boot won't able to bootstrap echo entry automatically from boot config file
func init() {
	rkentry.RegisterEntryRegFunc(RegisterMySqlEntriesWithConfig)
}

// BootConfig
// MySql entry boot config which reflects to YAML config
type BootConfig struct {
	MySql []struct {
		Enabled           bool     `yaml:"enabled" json:"enabled"`
		Name              string   `yaml:"name" json:"name"`
		Description       string   `yaml:"description" json:"description"`
		EnableMockDb      bool     `yaml:"enableMockDb" json:"enableMockDb"`
		EnableHealthCheck bool     `yaml:"enableHealthCheck" json:"enableHealthCheck"`
		Locale            string   `yaml:"locale" json:"locale"`
		User              string   `yaml:"user" json:"user"`
		Pass              string   `yaml:"pass" json:"pass"`
		Protocol          string   `yaml:"protocol" json:"protocol"`
		Addr              string   `yaml:"addr" json:"addr"`
		Database          string   `yaml:"database" json:"database"`
		Params            []string `yaml:"params" json:"params"`
		LoggerEncoding    string   `yaml:"loggerEncoding" json:"loggerEncoding"`
		LoggerOutputPaths []string `yaml:"loggerOutputPaths" json:"loggerOutputPaths"`
	} `yaml:"mySql" json:"mySql"`
}

// MySqlEntry will init gorm.DB or SqlMock with provided arguments
type MySqlEntry struct {
	EntryName         string                  `json:"entryName" yaml:"entryName"`
	EntryType         string                  `json:"entryType" yaml:"entryType"`
	EntryDescription  string                  `json:"-" yaml:"-"`
	zapLoggerEntry    *rkentry.ZapLoggerEntry `json:"-" yaml:"-"`
	loggerEncoding    int                     `json:"-" yaml:"-"`
	loggerOutputPath  []string                `json:"-" yaml:"-"`
	Logger            *zapgrpc.Logger         `json:"-" yaml:"-"`
	User              string                  `yaml:"user" json:"user"`
	pass              string                  `yaml:"-" json:"-"`
	protocol          string                  `yaml:"-" json:"-"`
	addr              string                  `yaml:"addr" json:"addr"`
	database          string                  `yaml:"database" json:"database"`
	params            []string                `yaml:"-" json:"-"`
	GormDB            *gorm.DB                `yaml:"-" json:"-"`
	GormConfig        *gorm.Config            `yaml:"-" json:"-"`
	GormAutoMigrate   []interface{}           `yaml:"-" json:"-"`
	enableMockDb      bool                    `yaml:"enableMockDb" json:"enableMockDb"`
	enableHealthCheck bool                    `yaml:"enableHealthCheck" json:"enableHealthCheck"`
	healthCheckTicker *time.Ticker            `yaml:"-" json:"-"`
	healthCheckSignal chan bool               `yaml:"-" json:"-"`
	SqlMock           sqlmock.Sqlmock         `yaml:"-" json:"-"`
	nowFunc           func() time.Time        `yaml:"-" json:"-"`
}

// DataStore will be extended in future.
type Option func(*MySqlEntry)

// WithName provide name.
func WithName(name string) Option {
	return func(entry *MySqlEntry) {
		entry.EntryName = name
	}
}

// WithDescription provide name.
func WithDescription(description string) Option {
	return func(entry *MySqlEntry) {
		entry.EntryDescription = description
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
			m.protocol = protocol
		}
	}
}

// WithAddr provide address
func WithAddr(addr string) Option {
	return func(m *MySqlEntry) {
		if len(addr) > 0 {
			m.addr = addr
		}
	}
}

// WithEnableHealthCheck enable health check or not
func WithEnableHealthCheck(enable bool) Option {
	return func(m *MySqlEntry) {
		m.enableHealthCheck = enable
	}
}

// WithDatabase provide database
func WithDatabase(database string) Option {
	return func(m *MySqlEntry) {
		if len(database) > 0 {
			m.database = database
		}
	}
}

// WithParams provide params
func WithParams(params ...string) Option {
	return func(m *MySqlEntry) {
		if len(params) > 0 {
			m.params = append(m.params, params...)
		}
	}
}

// WithEnableMockDb enables mock DB
func WithEnableMockDb(enable bool) Option {
	return func(m *MySqlEntry) {
		m.enableMockDb = enable
	}
}

// WithNowFunc provides now functions for unit test
func WithNowFunc(f func() time.Time) Option {
	return func(m *MySqlEntry) {
		m.nowFunc = f
	}
}

// WithZapLoggerEntry provide rkentry.ZapLoggerEntry.
func WithZapLoggerEntry(zapLoggerEntry *rkentry.ZapLoggerEntry) Option {
	return func(m *MySqlEntry) {
		if zapLoggerEntry != nil {
			m.zapLoggerEntry = zapLoggerEntry
		}
	}
}

// WithLoggerEncoding provide console=0, json=1.
// json or console is supported.
func WithLoggerEncoding(ec int) Option {
	return func(m *MySqlEntry) {
		m.loggerEncoding = ec
	}
}

// WithLoggerOutputPaths provide Logger Output Path.
// Multiple output path could be supported including stdout.
func WithLoggerOutputPaths(path ...string) Option {
	return func(m *MySqlEntry) {
		m.loggerOutputPath = append(m.loggerOutputPath, path...)
	}
}

// RegisterMySqlEntriesWithConfig register MySqlEntry based on config file into rkentry.GlobalAppCtx
func RegisterMySqlEntriesWithConfig(configFilePath string) map[string]rkentry.Entry {
	res := make(map[string]rkentry.Entry)

	// 1: unmarshal user provided config into boot config struct
	config := &BootConfig{}
	rkcommon.UnmarshalBootConfig(configFilePath, config)

	for _, element := range config.MySql {
		if len(element.Name) < 1 || !rkcommon.MatchLocaleWithEnv(element.Locale) {
			continue
		}

		opts := []Option{
			WithName(element.Name),
			WithDescription(element.Description),
			WithUser(element.User),
			WithPass(element.Pass),
			WithProtocol(element.Protocol),
			WithAddr(element.Addr),
			WithEnableHealthCheck(element.EnableHealthCheck),
			WithDatabase(element.Database),
			WithParams(element.Params...),
			WithEnableMockDb(element.EnableMockDb),
		}

		if strings.ToLower(element.LoggerEncoding) == "json" {
			opts = append(opts, WithLoggerEncoding(EncodingJson))
		}

		if len(element.LoggerOutputPaths) > 0 {
			opts = append(opts, WithLoggerOutputPaths(element.LoggerOutputPaths...))
		}

		entry := RegisterMySqlEntry(opts...)

		res[element.Name] = entry
	}

	return res
}

// RegisterEntry will register Entry into GlobalAppCtx
func RegisterMySqlEntry(opts ...Option) *MySqlEntry {
	entry := &MySqlEntry{
		EntryName:        "MySql",
		EntryType:        "MySql",
		EntryDescription: "",
		zapLoggerEntry:   rkentry.GlobalAppCtx.GetZapLoggerEntryDefault(),
		User:             "root",
		pass:             "pass",
		protocol:         "tcp",
		addr:             "localhost:3306",
		database:         "sys",
		params:           make([]string, 0),
		loggerOutputPath: make([]string, 0),
		GormConfig:       &gorm.Config{},
		GormAutoMigrate:  make([]interface{}, 0),
	}

	for i := range opts {
		opts[i](entry)
	}

	if len(entry.EntryDescription) < 1 {
		entry.EntryDescription = fmt.Sprintf("%s entry with name of %s, DB addr:%s, user:%s, DB:%s",
			entry.EntryType,
			entry.EntryName,
			entry.addr,
			entry.User,
			entry.database)
	}

	// Override zap logger encoding and output path if provided by user
	// Override encoding type
	if entry.loggerEncoding == EncodingJson || len(entry.loggerOutputPath) > 0 {
		zapConfig := copyZapLoggerConfig(entry.zapLoggerEntry.LoggerConfig)
		lumberjackConfig := entry.zapLoggerEntry.LumberjackConfig

		if entry.loggerEncoding == EncodingJson {
			zapConfig.Encoding = "json"
		}

		if len(entry.loggerOutputPath) > 0 {
			zapConfig.OutputPaths = toAbsPath(entry.loggerOutputPath...)
		}

		if lumberjackConfig == nil {
			lumberjackConfig = rklogger.NewLumberjackConfigDefault()
		}

		if logger, err := rklogger.NewZapLoggerWithConf(zapConfig, lumberjackConfig); err != nil {
			rkcommon.ShutdownWithError(err)
		} else {
			entry.Logger = zapgrpc.NewLogger(logger)
		}
	} else {
		entry.Logger = zapgrpc.NewLogger(entry.zapLoggerEntry.Logger)
	}

	entry.GormConfig.Logger = logger.New(entry.Logger, logger.Config{
		SlowThreshold:             200 * time.Millisecond,
		LogLevel:                  logger.Warn,
		IgnoreRecordNotFoundError: false,
		Colorful:                  false,
	})

	if entry.enableHealthCheck {
		entry.healthCheckTicker = time.NewTicker(5 * time.Second)
		entry.healthCheckSignal = make(chan bool)
	}

	rkentry.GlobalAppCtx.AddEntry(entry)

	return entry
}

// Bootstrap MySqlEntry
func (entry *MySqlEntry) Bootstrap(ctx context.Context) {
	entry.Logger.Println("Bootstrap mysql entry")

	// Create db if missing
	if err := entry.createDbIfMissing(); err != nil {
		entry.Logger.Printf("failed to create database", zap.Error(err))
		rkcommon.ShutdownWithError(fmt.Errorf("failed to create database at %s:%s@%s(%s)/%s",
			entry.User, "****", entry.protocol, entry.addr, entry.database))
	}

	// Connect to db
	if err := entry.connect(); err != nil {
		entry.Logger.Printf("failed to connect database", zap.Error(err))
		rkcommon.ShutdownWithError(fmt.Errorf("failed to open database at %s:%s@%s(%s)/%s",
			entry.User, "****", entry.protocol, entry.addr, entry.database))
	}

	// Auth migrate
	if err := entry.GormDB.AutoMigrate(entry.GormAutoMigrate...); err != nil {
		entry.Logger.Printf("failed to auth migrate", zap.Error(err))
		rkcommon.ShutdownWithError(fmt.Errorf("failed to auth migrate"))
	}

	// Health check
	if entry.enableHealthCheck {
		go func() {
			entry.healthCheck(context.TODO())
		}()
	}
}

// Interrupt MySqlEntry
func (entry *MySqlEntry) Interrupt(ctx context.Context) {
	entry.Logger.Println("Interrupt mysql entry")

	if entry.enableHealthCheck {
		entry.healthCheckSignal <- true
	}
}

// GetName returns entry name
func (entry *MySqlEntry) GetName() string {
	return entry.EntryName
}

// GetType returns entry type
func (entry *MySqlEntry) GetType() string {
	return entry.EntryType
}

// GetDescription returns entry description
func (entry *MySqlEntry) GetDescription() string {
	return entry.EntryDescription
}

// String returns json marshalled string
func (entry *MySqlEntry) String() string {
	bytes, err := json.Marshal(entry)
	if err != nil || len(bytes) < 1 {
		return "{}"
	}

	return string(bytes)
}

// Connect to to remote/local provider
func (entry *MySqlEntry) connect() error {
	// init gorm.DB
	sqlParams := ""
	for i := range entry.params {
		sqlParams += entry.params[i] + "&"
	}
	sqlParams = strings.TrimSuffix(sqlParams, "&")

	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s?%s",
		entry.User, entry.pass, entry.protocol, entry.addr, entry.database, sqlParams)

	var db *gorm.DB
	var err error

	if entry.enableMockDb {
		// Mock db enabled for unit test
		var sqlDb *sql.DB
		sqlDb, entry.SqlMock, _ = sqlmock.New()
		db, err = gorm.Open(mysql.New(mysql.Config{
			Conn:                      sqlDb,
			SkipInitializeWithVersion: true,
		}), &gorm.Config{
			NowFunc: entry.nowFunc,
		})
	} else {
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	}

	if err != nil {
		return err
	}

	entry.GormDB = db
	return nil
}

// IsHealthy checks healthy status remote provider
func (entry *MySqlEntry) IsHealthy() bool {
	if entry.enableMockDb {
		return true
	}

	if d, err := entry.GormDB.DB(); err != nil {
		return false
	} else {
		if err := d.Ping(); err != nil {
			return false
		}
	}

	return true
}

// HealthCheck runs periodic jobs which pings DB
func (entry *MySqlEntry) healthCheck(ctx context.Context) {
	for {
		select {
		case <-entry.healthCheckSignal:
			entry.Logger.Println("health check interrupted")
			return
		case <-entry.healthCheckTicker.C:
			if healthy := entry.IsHealthy(); !healthy {
				entry.Logger.Println("health check failed!")
			}
		}
	}
}

// AddAutoMigrate add gorm models needs to be auto migrated
func (entry *MySqlEntry) AddAutoMigrate(inters ...interface{}) {
	entry.GormAutoMigrate = append(entry.GormAutoMigrate, inters...)
}

// Create database if missing
func (entry *MySqlEntry) createDbIfMissing() error {
	// init gorm.DB
	sqlParams := ""
	for i := range entry.params {
		sqlParams += entry.params[i] + "&"
	}
	sqlParams = strings.TrimSuffix(sqlParams, "&")

	dsn := fmt.Sprintf("%s:%s@%s(%s)/?%s",
		entry.User, entry.pass, entry.protocol, entry.addr, sqlParams)

	var db *gorm.DB
	var err error

	if entry.enableMockDb {
		// Mock db enabled for unit test
		var sqlDb *sql.DB
		sqlDb, entry.SqlMock, _ = sqlmock.New()
		db, err = gorm.Open(mysql.New(mysql.Config{
			Conn:                      sqlDb,
			SkipInitializeWithVersion: true,
		}), entry.GormConfig)

		// For unit test
		entry.SqlMock.ExpectExec(
			fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4;", entry.database)).
			WillReturnResult(driver.RowsAffected(0))
	} else {
		db, err = gorm.Open(mysql.Open(dsn), entry.GormConfig)
	}

	if err != nil {
		return err
	}

	createSQL := fmt.Sprintf(
		"CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4;",
		entry.database,
	)

	if err := db.Exec(createSQL).Error; err != nil {
		return err
	}

	return nil
}

// Make incoming paths to absolute path with current working directory attached as prefix
func toAbsPath(p ...string) []string {
	res := make([]string, 0)

	for i := range p {
		if path.IsAbs(p[i]) {
			res = append(res, p[i])
		}
		wd, _ := os.Getwd()
		res = append(res, path.Join(wd, p[i]))
	}

	return res
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

// GetMySqlEntry returns MySqlEntry instance
func GetMySqlEntry(name string) *MySqlEntry {
	if raw := rkentry.GlobalAppCtx.GetEntry(name); raw != nil {
		if entry, ok := raw.(*MySqlEntry); ok {
			return entry
		}
	}

	return nil
}
