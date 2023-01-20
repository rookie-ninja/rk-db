// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.

package rkpostgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/rookie-ninja/rk-entry/v2/middleware"
	"go.uber.org/zap"
	gormLogger "gorm.io/gorm/logger"
	"gorm.io/gorm/utils"
	"runtime"
	"time"
)

var (
	infoStr      = "%s"
	warnStr      = "%s"
	errStr       = "%s"
	traceStr     = "[%.3fms] [rows:%v] %s"
	traceWarnStr = "%s\t[%.3fms] [rows:%v] %s"
	traceErrStr  = "%s\t[%.3fms] [rows:%v] %s"
)

type Logger struct {
	delegate                  *zap.Logger
	SlowThreshold             time.Duration
	IgnoreRecordNotFoundError bool
	LogLevel                  gormLogger.LogLevel
}

func (l *Logger) LogMode(level gormLogger.LogLevel) gormLogger.Interface {
	newLogger := *l
	newLogger.LogLevel = level
	return &newLogger
}

func (l *Logger) Info(ctx context.Context, msg string, data ...interface{}) {
	logger := l.delegate

	if v := ctx.Value(rkmid.LoggerKey.String()); v != nil {
		if loggerFromCtx, ok := v.(*zap.Logger); ok {
			logger = loggerFromCtx
		}
	}

	fileStack := utils.FileWithLineNum()
	logger = logger.WithOptions(zap.AddCallerSkip(linesToSkip(fileStack)))

	res := fmt.Sprintf(msg, data...)
	if len(res) > 200 {
		// split and concat
		res = res[:200] + "..."
	}

	if l.LogLevel >= gormLogger.Error {
		logger.Info(res)
	}
}

func (l *Logger) Warn(ctx context.Context, msg string, data ...interface{}) {
	logger := l.delegate

	if v := ctx.Value(rkmid.LoggerKey.String()); v != nil {
		if loggerFromCtx, ok := v.(*zap.Logger); ok {
			logger = loggerFromCtx
		}
	}

	fileStack := utils.FileWithLineNum()
	logger = logger.WithOptions(zap.AddCallerSkip(linesToSkip(fileStack)))

	res := fmt.Sprintf(msg, data...)
	if len(res) > 200 {
		// split and concat
		res = res[:200] + "..."
	}

	if l.LogLevel >= gormLogger.Error {
		logger.Warn(res)
	}
}

func (l *Logger) Error(ctx context.Context, msg string, data ...interface{}) {
	logger := l.delegate

	if v := ctx.Value(rkmid.LoggerKey.String()); v != nil {
		if loggerFromCtx, ok := v.(*zap.Logger); ok {
			logger = loggerFromCtx
		}
	}

	fileStack := utils.FileWithLineNum()
	logger = logger.WithOptions(zap.AddCallerSkip(linesToSkip(fileStack)))

	res := fmt.Sprintf(msg, data...)
	if len(res) > 200 {
		// split and concat
		res = res[:200] + "..."
	}

	if l.LogLevel >= gormLogger.Error {
		logger.Error(res)
	}
}

func (l *Logger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	logger := l.delegate

	if v := ctx.Value(rkmid.LoggerKey.String()); v != nil {
		if loggerFromCtx, ok := v.(*zap.Logger); ok {
			logger = loggerFromCtx
		}
	}

	fileStack := utils.FileWithLineNum()
	logger = logger.WithOptions(zap.AddCallerSkip(linesToSkip(fileStack)))

	elapsed := time.Since(begin)
	sql, rows := fc()
	// trim sql
	if len(sql) > 200 {
		sql = sql[:200] + "..."
	}

	switch {
	case err != nil && l.LogLevel >= gormLogger.Error && (!errors.Is(err, gormLogger.ErrRecordNotFound) || !l.IgnoreRecordNotFoundError):
		if rows == -1 {
			logger.Error(fmt.Sprintf(traceErrStr, err, float64(elapsed.Nanoseconds())/1e6, "-", sql))
		} else {
			logger.Error(fmt.Sprintf(traceErrStr, err, float64(elapsed.Nanoseconds())/1e6, rows, sql))
		}
	case elapsed > l.SlowThreshold && l.SlowThreshold != 0 && l.LogLevel >= gormLogger.Warn:
		slowLog := fmt.Sprintf("SLOW SQL >= %v", l.SlowThreshold)
		if rows == -1 {
			logger.Warn(fmt.Sprintf(traceWarnStr, slowLog, float64(elapsed.Nanoseconds())/1e6, "-", sql))
		} else {
			logger.Warn(fmt.Sprintf(traceWarnStr, slowLog, float64(elapsed.Nanoseconds())/1e6, rows, sql))
		}
	case l.LogLevel == gormLogger.Info:
		if rows == -1 {
			logger.Info(fmt.Sprintf(traceStr, float64(elapsed.Nanoseconds())/1e6, "-", sql))
		} else {
			logger.Info(fmt.Sprintf(traceStr, float64(elapsed.Nanoseconds())/1e6, rows, sql))
		}
	}

	return
}

func linesToSkip(f string) int {
	// the second caller usually from gorm internal, so set i start from 2
	for i := 2; i < 17; i++ {
		_, file, line, ok := runtime.Caller(i)
		if ok && fmt.Sprintf("%s:%d", file, line) == f {
			return i - 1
		}
	}

	return 0
}

func (l *Logger) getLogger(ctx context.Context) *zap.Logger {
	logger := l.delegate

	if v := ctx.Value(rkmid.LoggerKey.String()); v != nil {
		if loggerFromCtx, ok := v.(*zap.Logger); ok {
			logger = loggerFromCtx
		}
	}

	fileStack := utils.FileWithLineNum()
	callerSkip := zap.AddCallerSkip(linesToSkip(fileStack))

	return logger.WithOptions(callerSkip)
}
