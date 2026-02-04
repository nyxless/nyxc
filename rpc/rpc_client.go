package rpc

import (
	"context"
	"fmt"
	"github.com/nyxless/nyx/x"
	"github.com/nyxless/nyx/x/pb"
	"google.golang.org/grpc"
)

type Stream = pb.NYXRpc_CallStreamClient

type RpcClient struct {
	Address string
	client  pb.NYXRpcClient
	conn    *grpc.ClientConn
	rpcFail bool
}

func (c *RpcClient) Close() error {
	return c.conn.Close()
}

func (c *RpcClient) Request(ctx context.Context, method string, params map[string]any, opts []grpc.CallOption) (*pb.Reply, error) { //{{{
	request_data := c.prepare(params)
	reply, err := c.client.Call(ctx, &pb.Request{Method: method, Data: request_data}, opts...)

	if err != nil {
		c.rpcFail = true
		return nil, fmt.Errorf("grpc Call error: %+v", err)
	}

	return reply, nil
} // }}}

func (c *RpcClient) RequestStream(ctx context.Context, method string, params map[string]any, opts []grpc.CallOption) (Stream, error) { //{{{
	request_data := c.prepare(params)
	stream, err := c.client.CallStream(ctx, &pb.Request{Method: method, Data: request_data}, opts...)

	if err != nil {
		c.rpcFail = true
		return nil, fmt.Errorf("grpc CallStream error: %+v", err)
	}

	return stream, nil
} // }}}

func (c *RpcClient) prepare(params map[string]any) []*pb.Field { //{{{
	var data []*pb.Field
	for k, v := range params {
		field := &pb.Field{}
		field.Name = k
		field.Type, field.Value = x.DataToBytes(v)

		data = append(data, field)
	}

	return data
} // }}}
