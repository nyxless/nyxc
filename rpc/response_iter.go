package rpc

import (
	"context"
	"fmt"
	"github.com/nyxless/nyx/x/log"
	"google.golang.org/grpc/metadata"
	"io"
)

type ResponseIter struct {
	ctx     context.Context
	cancel  context.CancelFunc
	stream  Stream
	header  metadata.MD
	trailer metadata.MD
	logger  *log.Logger
}

func NewResponseIter(ctx context.Context, cancel context.CancelFunc, stream Stream, header, trailer metadata.MD, logger *log.Logger) *ResponseIter { // {{{
	return &ResponseIter{
		ctx:     ctx,
		cancel:  cancel,
		stream:  stream,
		header:  header,
		trailer: trailer,
		logger:  logger,
	}
} // }}}

// 遍历每行数据，直到处理完所有行或回调返回错误
func (ri *ResponseIter) Foreach(fn func(*Response) error) error { // {{{
	defer ri.cancel()

	for {
		reply, err := ri.stream.Recv()

		if err == io.EOF {
			break // 流结束
		}

		if err != nil {
			return fmt.Errorf("error while reading stream: %+v", err)
		}

		response := NewResponse(ri.ctx, reply, ri.header, ri.trailer, ri.logger)

		if err := fn(response); err != nil {
			return err
		}
	}

	return nil
} // }}}

// 收集所有行数据到切片中
func (ri *ResponseIter) Collect() ([]*Response, error) { // {{{
	var result []*Response
	collectFn := func(res *Response) error {
		result = append(result, res)
		return nil
	}

	if err := ri.Foreach(collectFn); err != nil {
		return nil, err
	}

	return result, nil
} // }}}
