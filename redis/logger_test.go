// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.
package rkredis

import (
	"context"
	"github.com/rookie-ninja/rk-entry/entry"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLogger_Printf(t *testing.T) {
	// test with nil zap logger
	assert.NotNil(t, NewLogger(nil))

	// happy case
	logger := NewLogger(rkentry.GlobalAppCtx.GetZapLoggerDefault())
	assert.NotNil(t, logger)
	logger.Printf(context.TODO(), "%s", "arg")
}
