package plugins

import (
	"context"
	rkmidprom "github.com/rookie-ninja/rk-entry/v2/middleware/prom"
	"gorm.io/gorm"
	"strings"
	"time"
)

func toPromName(in string) string {
	in = strings.ReplaceAll(in, "-", "")
	in = strings.ReplaceAll(in, ":", "")
	return in
}

func NewProm(conf *PromConfig) *Prom {
	res := &Prom{
		MetricsSet: rkmidprom.NewMetricsSet("rk", toPromName(conf.DbType), nil),
		LabelKeys: []string{
			"database",
			"addr",
			"table",
			"action",
		},
		Conf: conf,
	}

	res.MetricsSet.RegisterCounter("rowsAffected", res.LabelKeys...)
	res.MetricsSet.RegisterCounter("error", res.LabelKeys...)
	res.MetricsSet.RegisterSummary("elapsedNano", rkmidprom.SummaryObjectives, res.LabelKeys...)

	return res
}

const (
	startTimeKey = "rk-startTime"
)

type PromConfig struct {
	Enabled bool   `yaml:"enabled" json:"enabled"`
	DbAddr  string `yaml:"-" json:"-"`
	DbName  string `yaml:"-" json:"-"`
	DbType  string `yaml:"-" json:"-"`
}

type Prom struct {
	MetricsSet *rkmidprom.MetricsSet
	LabelKeys  []string
	Conf       *PromConfig
}

func (p *Prom) Name() string {
	return "rk-prom-plugin"
}

func (p *Prom) before() func(db *gorm.DB) {
	return func(db *gorm.DB) {
		db.Statement.Context = context.WithValue(db.Statement.Context, startTimeKey, time.Now())
	}
}

func (p *Prom) after(action string) func(db *gorm.DB) {
	return func(db *gorm.DB) {
		endTimeRaw := db.Statement.Context.Value(startTimeKey)
		if endTimeRaw == nil {
			return
		}

		endTime, ok := endTimeRaw.(time.Time)
		if !ok {
			return
		}

		elapsed := time.Now().Sub(endTime).Nanoseconds()

		labelValues := []string{
			p.Conf.DbName,
			p.Conf.DbAddr,
			db.Statement.Table,
			action,
		}

		if observer, err := p.MetricsSet.GetSummary("elapsedNano").GetMetricWithLabelValues(labelValues...); err == nil {
			observer.Observe(float64(elapsed))
		}

		if counter, err := p.MetricsSet.GetCounter("rowsAffected").GetMetricWithLabelValues(labelValues...); err == nil && db.Statement.RowsAffected > 0 {
			counter.Add(float64(db.Statement.RowsAffected))
		}

		if counter, err := p.MetricsSet.GetCounter("error").GetMetricWithLabelValues(labelValues...); err == nil && db.Statement.Error != nil {
			counter.Inc()
		}

	}
}

func (p *Prom) Initialize(db *gorm.DB) error {
	// query
	if err := db.Callback().Query().Before("gorm:query").Register(":before_query", p.before()); err != nil {
		return err
	}
	if err := db.Callback().Query().After("gorm:query").Register(":after_query", p.after("query")); err != nil {
		return err
	}

	// create
	if err := db.Callback().Create().Before("gorm:create").Register(":before_create", p.before()); err != nil {
		return err
	}
	if err := db.Callback().Create().After("gorm:create").Register(":after_create", p.after("create")); err != nil {
		return err
	}

	// update
	if err := db.Callback().Update().Before("gorm:update").Register(":before_update", p.before()); err != nil {
		return err
	}
	if err := db.Callback().Update().After("gorm:update").Register(":after_update", p.after("update")); err != nil {
		return err
	}

	// delete
	if err := db.Callback().Delete().Before("gorm:delete").Register(":before_delete", p.before()); err != nil {
		return err
	}
	if err := db.Callback().Delete().After("gorm:delete").Register(":after_delete", p.after("delete")); err != nil {
		return err
	}

	// raw
	if err := db.Callback().Raw().Before("gorm:raw").Register(":before_raw", p.before()); err != nil {
		return err
	}
	if err := db.Callback().Raw().After("gorm:raw").Register(":after_raw", p.after("raw")); err != nil {
		return err
	}

	return nil
}
