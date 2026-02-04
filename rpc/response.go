package rpc

import (
	"context"
	"github.com/nyxless/nyx/x"
	"github.com/nyxless/nyx/x/log"
	"github.com/nyxless/nyx/x/pb"
	"google.golang.org/grpc/metadata"
	"time"
)

func NewResponse(ctx context.Context, reply *pb.Reply, header, trailer metadata.MD, logger *log.Logger) *Response {
	code := reply.Code
	msg := reply.Msg
	consume := reply.Consume
	server_time := reply.Time
	res := Process(reply.Data)

	appid := ctx.Value("appid")
	method := ctx.Value("method")
	params := ctx.Value("params")
	hit_cache, _ := ctx.Value("hit_cache").(bool)
	cache_time, _ := ctx.Value("cache_time").(time.Time)

	logger.Log("nyxclient", map[string]any{"appid": appid, "uri": method, "cache": hit_cache}, params, map[string]any{"code": code, "consume": consume, "data": res, "msg": msg, "time": server_time})
	if code > 0 {
		logger.Log("nyxclient-err", map[string]any{"appid": appid, "uri": method}, params, map[string]any{"code": code, "consume": consume, "data": res, "msg": msg, "time": server_time})
	}

	return &Response{
		Code:      code,
		Msg:       msg,
		Data:      res,
		Header:    header,
		Trailer:   trailer,
		HitCache:  hit_cache,
		CacheTime: cache_time,
	}
}

type Response struct {
	Data      map[string]any
	Code      int32
	Msg       string
	Header    metadata.MD
	Trailer   metadata.MD
	HitCache  bool
	CacheTime time.Time
}

// 获取返回结果的data对象
func (r *Response) GetData() map[string]any { // {{{
	return r.Data
} // }}}

// 获取返回结果的data对象中key对应的值
func (r *Response) GetValue(key string) any {
	return r.Data[key]
}

func (r *Response) GetCacheTime() (time.Time, bool) {
	return r.CacheTime, r.HitCache
}

func (r *Response) GetCode() int32 {
	return r.Code
}

func (r *Response) GetMsg() string {
	return r.Msg
}

func (r *Response) GetHeaders() metadata.MD {
	return r.Header
}

func (r *Response) GetHeader(k string) string { // {{{
	h := r.Header.Get(k)
	if len(h) > 0 {
		return h[0]
	}

	return ""
} // }}}

func (r *Response) GetTrailers() metadata.MD {
	return r.Trailer
}

func (r *Response) GetTrailer(k string) string { // {{{
	t := r.Trailer.Get(k)
	if len(t) > 0 {
		return t[0]
	}

	return ""
} // }}}

func Process(fields []*pb.Field) map[string]any { //{{{
	if len(fields) == 0 {
		return nil
	}

	res := map[string]any{}
	for _, field := range fields {
		res[field.Name] = x.BytesToData(field.Type, field.Value)
	}

	return res
} // }}}
