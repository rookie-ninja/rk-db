// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.

package rkredis

import (
	"context"
	"fmt"
	"github.com/rookie-ninja/rk-entry/v2/entry"
	"go.uber.org/zap"
)

func NewLogger(zapLogger *zap.Logger) *Logger {
	if zapLogger == nil {
		zapLogger = rkentry.LoggerEntryStdout.Logger
	}

	return &Logger{
		delegate: zapLogger,
	}
}

type Logger struct {
	delegate *zap.Logger
}

func (l Logger) Printf(ctx context.Context, format string, v ...interface{}) {
	l.delegate.Info(fmt.Sprintf(format, v...))
}
