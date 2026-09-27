// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.

package rkpostgres

import (
	"fmt"
	"sync"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// fastDialector wraps the gorm postgres dialector to cut round trips in AutoMigrate.
//
// A warm AutoMigrate issues one introspection query per column and per index (~4000 queries for a
// 100-table schema). Harmless locally, but against a remote database each query is a round trip:
// at 171ms RTT that is ~11 minutes. DDL and migration decisions are unchanged; only redundant
// introspection queries are removed.
type fastDialector struct {
	*postgres.Dialector
	indexes *indexCache
}

func newDialector(d gorm.Dialector) gorm.Dialector {
	if pd, ok := d.(*postgres.Dialector); ok {
		return fastDialector{Dialector: pd, indexes: &indexCache{tables: map[string]indexEntry{}}}
	}

	return d
}

func (d fastDialector) Migrator(db *gorm.DB) gorm.Migrator {
	return fastMigrator{Migrator: d.Dialector.Migrator(db).(postgres.Migrator), indexes: d.indexes}
}

type fastMigrator struct {
	postgres.Migrator
	indexes *indexCache
}

// MigrateColumn: the postgres driver looks up every column's comment (one query per column) but only
// uses the result when the field declares a comment. Skip the lookup when it doesn't.
func (m fastMigrator) MigrateColumn(value interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	if field.Comment != "" {
		return m.Migrator.MigrateColumn(value, field, columnType)
	}

	// same as the postgres driver: primary key columns are not diffed
	if field.PrimaryKey {
		return nil
	}

	return m.Migrator.Migrator.MigrateColumn(value, field, columnType)
}

// HasIndex loads all index names of a table in one query instead of one query per index.
func (m fastMigrator) HasIndex(value interface{}, name string) bool {
	found := false
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}
		currentSchema, curTable := m.CurrentSchema(stmt, stmt.Table)
		key := fmt.Sprint(currentSchema, ".", curTable)

		names, ok := m.indexes.get(key)
		if !ok {
			list := make([]string, 0)
			if err := m.DB.Raw(
				"SELECT indexname FROM pg_indexes WHERE tablename = ? AND schemaname = ?", curTable, currentSchema,
			).Scan(&list).Error; err != nil {
				return err
			}
			names = make(map[string]bool, len(list))
			for i := range list {
				names[list[i]] = true
			}
			m.indexes.set(key, names)
		}

		found = names[name]
		return nil
	})

	return found
}

// Anything that changes indexes invalidates the cache.

func (m fastMigrator) CreateIndex(value interface{}, name string) error {
	defer m.indexes.clear()
	return m.Migrator.CreateIndex(value, name)
}

func (m fastMigrator) DropIndex(value interface{}, name string) error {
	defer m.indexes.clear()
	return m.Migrator.DropIndex(value, name)
}

func (m fastMigrator) RenameIndex(value interface{}, oldName, newName string) error {
	defer m.indexes.clear()
	return m.Migrator.RenameIndex(value, oldName, newName)
}

func (m fastMigrator) CreateTable(values ...interface{}) error {
	defer m.indexes.clear()
	return m.Migrator.CreateTable(values...)
}

func (m fastMigrator) DropTable(values ...interface{}) error {
	defer m.indexes.clear()
	return m.Migrator.DropTable(values...)
}

// indexCache entries expire so a long-running process never trusts index state for long.
const indexCacheTTL = time.Minute

type indexEntry struct {
	names    map[string]bool
	loadedAt time.Time
}

type indexCache struct {
	lock   sync.Mutex
	tables map[string]indexEntry
}

func (c *indexCache) get(key string) (map[string]bool, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	e, ok := c.tables[key]
	if !ok || time.Since(e.loadedAt) > indexCacheTTL {
		return nil, false
	}

	return e.names, true
}

func (c *indexCache) set(key string, names map[string]bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.tables[key] = indexEntry{names: names, loadedAt: time.Now()}
}

func (c *indexCache) clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.tables = map[string]indexEntry{}
}
