// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.
package rkmysql

import (
	"context"
	"github.com/rookie-ninja/rk-entry/v2/entry"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRegisterMySqlEntry(t *testing.T) {
	// without options
	entry := RegisterMySqlEntry()

	assert.NotEmpty(t, entry.GetName())
	assert.NotEmpty(t, entry.GetType())
	assert.NotEmpty(t, entry.GetDescription())
	assert.NotEmpty(t, entry.String())
	assert.Equal(t, "root", entry.User)
	assert.Equal(t, "pass", entry.pass)
	assert.Equal(t, "tcp", entry.Protocol)
	assert.Equal(t, "localhost:3306", entry.Addr)
	assert.Empty(t, entry.GormDbMap)
	assert.Empty(t, entry.GormConfigMap)

	// remove entry
	rkentry.GlobalAppCtx.RemoveEntry(entry)

	// with options
	entry = RegisterMySqlEntry(
		WithName("ut-entry"),
		WithDescription("ut-entry"),
		WithUser("ut-user"),
		WithPass("ut-pass"),
		WithProtocol("ut-protocol"),
		WithAddr("ut-addr"),
		WithDatabase("ut-database", true, false))

	assert.Equal(t, "ut-entry", entry.GetName())
	assert.NotEmpty(t, entry.GetType())
	assert.Equal(t, "ut-entry", entry.GetDescription())
	assert.NotEmpty(t, entry.String())
	assert.Equal(t, "ut-user", entry.User)
	assert.Equal(t, "ut-pass", entry.pass)
	assert.Equal(t, "ut-protocol", entry.Protocol)
	assert.Equal(t, "ut-addr", entry.Addr)
	assert.Empty(t, entry.GormDbMap)
	assert.NotEmpty(t, entry.GormConfigMap)

	// remove entry
	rkentry.GlobalAppCtx.RemoveEntry(entry)
}

func TestMySqlEntry_IsHealthy(t *testing.T) {
	// test with dry run enabled
	entry := RegisterMySqlEntry()
	entry.Bootstrap(context.TODO())

	assert.True(t, entry.IsHealthy())

	defer rkentry.GlobalAppCtx.RemoveEntry(entry)
	defer entry.Interrupt(context.TODO())
}

func TestGetMySqlEntry(t *testing.T) {
	// expect nil
	assert.Nil(t, GetMySqlEntry("not-exist"))

	// with invalid entry
	assert.Nil(t, GetMySqlEntry(rkentry.GlobalAppCtx.GetAppInfoEntry().GetName()))

	entry := RegisterMySqlEntry()
	defer rkentry.GlobalAppCtx.RemoveEntry(entry)
	// happy case
	assert.Equal(t, entry, GetMySqlEntry(entry.GetName()))
}

func TestRegisterMySqlEntriesWithConfig(t *testing.T) {
	bootConfigStr := `
mysql:
  - name: user-db
    enabled: true
    locale: "*::*::*::*"
    addr: "localhost:3306"
    user: root
    pass: pass
    database:
      - name: user
        autoCreate: true
        dryRun: false
        params:
          - "charset=utf8mb4"
          - "parseTime=True"
          - "loc=Local"
`

	entries := RegisterMySqlEntryYAML([]byte(bootConfigStr))

	assert.NotEmpty(t, entries)

	rkentry.GlobalAppCtx.RemoveEntry(entries["user-db"])
}

func TestMySqlEntry_Bootstrap(t *testing.T) {
	defer assertPanic(t)

	entry := RegisterMySqlEntry(
		WithAddr("fake-addr"),
		WithDatabase("ut-database", false, true))
	entry.Bootstrap(context.TODO())

	assert.NotNil(t, entry.GetDB("ut-database"))
	assert.True(t, entry.IsHealthy())
}

func assertNotPanic(t *testing.T) {
	if r := recover(); r != nil {
		// Expect panic to be called with non nil error
		assert.True(t, false)
	} else {
		// This should never be called in case of a bug
		assert.True(t, true)
	}
}

func assertPanic(t *testing.T) {
	if r := recover(); r != nil {
		// Expect panic to be called with non nil error
		assert.True(t, true)
	} else {
		// This should never be called in case of a bug
		assert.True(t, false)
	}
}
