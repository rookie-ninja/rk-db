package rkpostgres

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
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

type migV1 struct {
	ID uint   `gorm:"primaryKey"`
	A  string `gorm:"index:idx_mig_a"`
	B  string `gorm:"comment:hello"`
}

func (migV1) TableName() string { return "rk_mig" }

type migV2 struct {
	ID uint   `gorm:"primaryKey"`
	A  string `gorm:"index:idx_mig_a"`
	B  string `gorm:"comment:world"`
	C  string `gorm:"index:idx_mig_c"`
	D  int
}

func (migV2) TableName() string { return "rk_mig" }

type countLogger struct {
	gormLogger.Interface
	n *int
}

func (l countLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	*l.n++
}

// Requires a real PostgreSQL: RK_TEST_PG_DSN. fastDialector migrates exactly like the plain dialector
// (new column / index / comment all applied) with fewer introspection queries.
func TestFastDialectorMigrates(t *testing.T) {
	dsn := os.Getenv("RK_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("RK_TEST_PG_DSN not set")
	}

	open := func(fast bool, n *int) *gorm.DB {
		d := postgres.Open(dsn)
		if fast {
			d = newDialector(d)
		}
		db, err := gorm.Open(d, &gorm.Config{Logger: countLogger{Interface: gormLogger.Discard, n: n}})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}

	var n int
	db := open(true, &n)
	db.Migrator().DropTable("rk_mig")
	if err := db.AutoMigrate(&migV1{}); err != nil {
		t.Fatal(err)
	}
	if !db.Migrator().HasIndex(&migV1{}, "idx_mig_a") || db.Migrator().HasIndex(&migV1{}, "idx_mig_nope") {
		t.Fatal("HasIndex wrong after create")
	}

	// schema change: new column, new index, changed comment
	if err := db.AutoMigrate(&migV2{}); err != nil {
		t.Fatal(err)
	}
	if !db.Migrator().HasColumn(&migV2{}, "d") || !db.Migrator().HasIndex(&migV2{}, "idx_mig_c") {
		t.Fatal("new column / index not migrated")
	}
	var comment string
	db.Raw("SELECT col_description('rk_mig'::regclass, (SELECT ordinal_position FROM information_schema.columns WHERE table_name='rk_mig' AND column_name='b'))").Scan(&comment)
	if comment != "world" {
		t.Fatalf("comment not migrated: %q", comment)
	}

	// warm migrate: same result, fewer queries
	var fastN, plainN int
	if err := open(true, &fastN).AutoMigrate(&migV2{}); err != nil {
		t.Fatal(err)
	}
	if err := open(false, &plainN).AutoMigrate(&migV2{}); err != nil {
		t.Fatal(err)
	}
	t.Logf("warm AutoMigrate queries: fast=%d plain=%d", fastN, plainN)
	if fastN >= plainN {
		t.Fatalf("fast dialector should issue fewer queries: fast=%d plain=%d", fastN, plainN)
	}
}
