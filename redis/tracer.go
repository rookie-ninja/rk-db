package rkredis

import (
	"context"
	"github.com/go-redis/redis/extra/rediscmd/v8"
	"github.com/go-redis/redis/v8"
	"github.com/rookie-ninja/rk-entry/middleware"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var noopTracerProvider = trace.NewNoopTracerProvider()

type RedisTracer struct{}

func NewRedisTracer() *RedisTracer {
	return new(RedisTracer)
}

func (t *RedisTracer) getTracer(ctx context.Context) trace.Tracer {
	if v := ctx.Value(rkmid.TracerKey); v != nil {
		if res, ok := v.(trace.Tracer); ok {
			return res
		}
	}

	return noopTracerProvider.Tracer("trace-noop")
}

func (t *RedisTracer) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, nil
	}

	tracer := t.getTracer(ctx)

	ctx, span := tracer.Start(ctx, cmd.FullName())
	span.SetAttributes(
		attribute.String("db.system", "redis"),
		attribute.String("db.statement", rediscmd.CmdString(cmd)),
	)

	return ctx, nil
}

func (t *RedisTracer) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	span := trace.SpanFromContext(ctx)
	if err := cmd.Err(); err != nil {
		recordError(ctx, span, err)
	}
	span.End()
	return nil
}

func (t *RedisTracer) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, nil
	}

	tracer := t.getTracer(ctx)

	summary, cmdsString := rediscmd.CmdsString(cmds)

	ctx, span := tracer.Start(ctx, "pipeline "+summary)
	span.SetAttributes(
		attribute.String("db.system", "redis"),
		attribute.Int("db.redis.num_cmd", len(cmds)),
		attribute.String("db.statement", cmdsString),
	)

	return ctx, nil
}

func (t *RedisTracer) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	span := trace.SpanFromContext(ctx)
	if err := cmds[0].Err(); err != nil {
		recordError(ctx, span, err)
	}
	span.End()
	return nil
}

func recordError(ctx context.Context, span trace.Span, err error) {
	if err != redis.Nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
