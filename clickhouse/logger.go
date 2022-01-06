// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.
package rkclickhouse

import (
	"fmt"
	"github.com/rookie-ninja/rk-entry/entry"
	"go.uber.org/zap"
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
	l.delegate.Info(fmt.Sprintf(s, i...))
}
