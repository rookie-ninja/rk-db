package rkredis

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewRedisTracer(t *testing.T) {
	assert.NotNil(t, NewRedisTracer())
}

func TestRedisTracer_BeforeProcess(t *testing.T) {
	tracer := NewRedisTracer()

	ctx, err := tracer.BeforeProcess(context.TODO(), redis.NewStringCmd(context.TODO()))
	assert.NotNil(t, ctx)
	assert.Nil(t, err)
}

func TestRedisTracer_AfterProcess(t *testing.T) {
	tracer := NewRedisTracer()

	err := tracer.AfterProcess(context.TODO(), redis.NewStringCmd(context.TODO()))
	assert.Nil(t, err)
}

func TestRedisTracer_AfterProcessPipeline(t *testing.T) {
	tracer := NewRedisTracer()

	ctx, err := tracer.BeforeProcessPipeline(context.TODO(), []redis.Cmder{redis.NewStringCmd(context.TODO())})
	assert.NotNil(t, ctx)
	assert.Nil(t, err)
}
