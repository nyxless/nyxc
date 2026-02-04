package rpc

import (
	"context"
	"google.golang.org/grpc"
	"time"
)

// 缓存更新成功后回调函数, 参数: 返回值中 data 字段
type CacheCallbackFn func(map[string]any) error

type RpcOptions struct {
	Timeout              time.Duration
	MaxIdleConns         int
	MaxOpenConns         int
	Headers              map[string]string
	Ctx                  context.Context
	UseCache             bool
	CacheSize            int
	CacheTtl             int
	CacheRefreshInterval int
	CacheCallback        CacheCallbackFn
	Debug                bool
	AuthFn               func(appid, secret string) map[string]string
}

type DialOption struct {
	grpc.EmptyDialOption
	F func(*RpcOptions)
}

type CallOption struct {
	grpc.EmptyCallOption
	F func(*RpcOptions)
}
