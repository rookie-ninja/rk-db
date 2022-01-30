// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.
package rkpostgres

import (
	"fmt"
	"github.com/rookie-ninja/rk-entry/entry"
	"go.uber.org/zap"
	"strings"
)

func NewLogger(zapLogger *zap.Logger) *Logger {
	if zapLogger == nil {
		zapLogger = rkentry.GlobalAppCtx.GetZapLoggerDefault()
	}

	return &Logger{
		delegate: zapLogger,
	}
}

type Logger struct {
	delegate *zap.Logger
}

func (l Logger) Printf(s string, i ...interface{}) {
	if strings.Contains(s, "[error]") {
		l.delegate.Error(fmt.Sprintf(s, i...))
	} else if strings.Contains(s, "[warn]") {
		l.delegate.Warn(fmt.Sprintf(s, i...))
	} else {
		l.delegate.Info(fmt.Sprintf(s, i...))
	}
}
