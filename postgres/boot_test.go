package rkpostgres

import (
	"context"
	"fmt"
	"os"
	"testing"
)

// Requires a real PostgreSQL: RK_TEST_PG_ADDR=127.0.0.1:5432 (user postgres, empty password, trust auth).
func TestSharedPhysicalDatabase(t *testing.T) {
	addr := os.Getenv("RK_TEST_PG_ADDR")
	if addr == "" {
		t.Skip("RK_TEST_PG_ADDR not set")
	}

	raw := []byte(fmt.Sprintf(`
postgres:
  - name: main
    enabled: true
    user: postgres
    pass: unused
    addr: "%[1]s"
    database:
      - name: RkMain
        autoCreate: true
        maxOpenConn: 7
  - name: prov
    enabled: true
    user: postgres
    pass: unused
    addr: "%[1]s"
    database:
      - name: RkProvShared
        autoCreate: true
        maxOpenConn: 3
        params: ["sslmode=disable", "TimeZone=Asia/Shanghai", "dbname=RkMain"]
      - name: RkProvOwn
        autoCreate: true
`, addr))

	entries := RegisterPostgresEntryYAML(raw)
	entries["main"].Bootstrap(context.Background()) // main first: it owns the pool size
	entries["prov"].Bootstrap(context.Background())

	mainDB := GetPostgresEntry("main").GetDB("RkMain")
	sharedDB := GetPostgresEntry("prov").GetDB("RkProvShared")
	ownDB := GetPostgresEntry("prov").GetDB("RkProvOwn")

	mainPool, _ := mainDB.DB()
	sharedPool, _ := sharedDB.DB()
	ownPool, _ := ownDB.DB()

	if mainPool != sharedPool {
		t.Fatal("dbname=RkMain should share the main pool")
	}
	if got := mainPool.Stats().MaxOpenConnections; got != 7 {
		t.Fatalf("pool size should come from the first opener (7), got %d", got)
	}
	if ownPool == mainPool {
		t.Fatal("database without dbname param must keep its own pool")
	}

	var current string
	sharedDB.Raw("SELECT current_database()").Scan(&current)
	if current != "RkMain" {
		t.Fatalf("RkProvShared connected to %q, want RkMain", current)
	}
	ownDB.Raw("SELECT current_database()").Scan(&current)
	if current != "RkProvOwn" {
		t.Fatalf("RkProvOwn connected to %q, want RkProvOwn", current)
	}

	var n int64
	mainDB.Raw("SELECT count(*) FROM pg_database WHERE datname = 'RkProvShared'").Scan(&n)
	if n != 0 {
		t.Fatal("logical database RkProvShared must not be created physically")
	}
}
