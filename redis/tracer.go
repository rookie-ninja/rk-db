// Copyright (c) 2021 rookie-ninja
//
// Use of this source code is governed by an Apache-style
// license that can be found in the LICENSE file.

package rkredis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/extra/rediscmd/v9"
	"github.com/redis/go-redis/v9"
	"github.com/rookie-ninja/rk-entry/v2/middleware"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"net"
)

var noopTracerProvider = trace.NewNoopTracerProvider()

type RedisTracer struct{}

func NewRedisTracer() *RedisTracer {
	return new(RedisTracer)
}

func (t *RedisTracer) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if !trace.SpanFromContext(ctx).IsRecording() {
			return next(ctx, network, addr)
		}

		tracer := t.getTracer(ctx)

		ctx, span := tracer.Start(ctx, fmt.Sprintf("%s::%s", network, addr))
		span.SetAttributes(
			attribute.String("db.system", "redis"),
			attribute.String("db.statement", "dial"),
		)

		conn, err := next(ctx, network, addr)

		if err != nil {
			recordError(ctx, span, err)
		}
		span.End()

		return conn, err
	}
}

func (t *RedisTracer) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if !trace.SpanFromContext(ctx).IsRecording() {
			return next(ctx, cmd)
		}

		tracer := t.getTracer(ctx)

		ctx, span := tracer.Start(ctx, cmd.FullName())
		span.SetAttributes(
			attribute.String("db.system", "redis"),
			attribute.String("db.statement", rediscmd.CmdString(cmd)),
		)

		err := next(ctx, cmd)

		if err != nil {
			recordError(ctx, span, err)
		}
		span.End()

		return err
	}
}

func (t *RedisTracer) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if !trace.SpanFromContext(ctx).IsRecording() {
			return next(ctx, cmds)
		}

		tracer := t.getTracer(ctx)

		key, _ := rediscmd.CmdsString(cmds)
		ctx, span := tracer.Start(ctx, key)
		span.SetAttributes(
			attribute.String("db.system", "redis"),
			attribute.Int("db.redis.num_cmd", len(cmds)),
		)

		for i := range cmds {
			cmd := cmds[i]
			span.SetAttributes(
				attribute.String(fmt.Sprintf("db.statement.%d", i), rediscmd.CmdString(cmd)),
			)
		}

		err := next(ctx, cmds)

		if err != nil {
			recordError(ctx, span, err)
		}
		span.End()

		return err
	}
}

func (t *RedisTracer) getTracer(ctx context.Context) trace.Tracer {
	if v := ctx.Value(rkmid.TracerKey); v != nil {
		if res, ok := v.(trace.Tracer); ok {
			return res
		}
	}

	return noopTracerProvider.Tracer("trace-noop")
}

func recordError(ctx context.Context, span trace.Span, err error) {
	if err != redis.Nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
