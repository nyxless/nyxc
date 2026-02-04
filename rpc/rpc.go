package rpc

import (
	"context"
	"fmt"
	"github.com/nyxless/nyx/x/pb"
	"google.golang.org/grpc"
	"sync"
	"time"
)

type RpcPool struct {
	Addr         string
	pool         chan *RpcClient
	df           DialFunc
	options      []grpc.DialOption
	lock         sync.RWMutex
	connRequests int
	maxIdleConns int
	maxOpenConns int
}

var Debug bool

type DialFunc func(addr string, opts []grpc.DialOption) (*RpcClient, error)

func NewRpcPool(addr string, max_idle_conns, max_open_conns int, df DialFunc, opts []grpc.DialOption) (*RpcPool, error) { // {{{
	var client *RpcClient
	var err error

	pool := make([]*RpcClient, 0, max_idle_conns)
	for i := 0; i < max_idle_conns; i++ {
		client, err = df(addr, opts)
		if err != nil {
			for _, client = range pool {
				client.Close()
			}
			pool = pool[0:]
			break
		}
		pool = append(pool, client)
	}
	p := RpcPool{
		Addr:         addr,
		pool:         make(chan *RpcClient, max_open_conns),
		df:           df,
		options:      opts,
		maxOpenConns: max_open_conns,
		maxIdleConns: max_idle_conns,
	}
	for i := range pool {
		p.pool <- pool[i]
	}
	return &p, err
} // }}}

func New(addr string, max_idle_conns, max_open_conns int, opts []grpc.DialOption) (*RpcPool, error) {
	return NewRpcPool(addr, max_idle_conns, max_open_conns, DialGrpc, opts)
}

// 从连接池中取已建立的连接，不存在则创建
func (p *RpcPool) Get() (*RpcClient, error) { // {{{
	Println("get ==== > Requests :", p.connRequests, "PoolSize:", cap(p.pool), " Avail:", len(p.pool))
	p.lock.Lock()
	p.connRequests++
	if p.connRequests > p.maxOpenConns {
		p.lock.Unlock()
		Println("wait...")
		select {
		case <-time.After(3 * time.Second):
			return nil, fmt.Errorf("pool is full")
		case conn := <-p.pool:
			return conn, nil
		}
	} else {
		p.lock.Unlock()
		select {
		case <-time.After(3 * time.Second):
			return nil, fmt.Errorf("pool is full")
		case conn := <-p.pool:
			return conn, nil
		default:
			Println("gen a new conn")
			return p.df(p.Addr, p.options)
		}
	}
} // }}}

// 将连接放回池子
func (p *RpcPool) Put(conn *RpcClient) { // {{{
	p.lock.RLock()
	//当前可用连接数大于最大空闲数则直接抛弃
	if len(p.pool) >= p.maxIdleConns {
		p.lock.RUnlock()
		conn.Close()
	} else {
		p.lock.RUnlock()

		if !conn.rpcFail {
			select {
			case p.pool <- conn:
			default:
				conn.Close()
			}
		} else {
			fmt.Errorf("rpcClient error")
			conn.Close()
		}
	}

	p.lock.Lock()
	p.connRequests--
	p.lock.Unlock()

	Println("put ==== > Requests :", p.connRequests, "PoolSize:", cap(p.pool), " Avail:", len(p.pool))
} // }}}

func (p *RpcPool) Request(ctx context.Context, method string, params map[string]any, opts []grpc.CallOption) (*pb.Reply, error) { // {{{
	c, err := p.Get()
	if err != nil {
		return nil, err
	}
	defer p.Put(c)

	return c.Request(ctx, method, params, opts)
} // }}}

func (p *RpcPool) RequestStream(ctx context.Context, method string, params map[string]any, opts []grpc.CallOption) (Stream, error) { // {{{
	c, err := p.Get()
	if err != nil {
		return nil, err
	}
	defer p.Put(c)

	return c.RequestStream(ctx, method, params, opts)
} // }}}

// 清空池子并关闭连接
func (p *RpcPool) Empty() { // {{{
	var conn *RpcClient
	for {
		select {
		case conn = <-p.pool:
			conn.Close()
		default:
			return
		}
	}
} // }}}

func DialGrpc(addr string, opts []grpc.DialOption) (*RpcClient, error) { // {{{
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &RpcClient{
		Address: addr,
		client:  pb.NewNYXRpcClient(conn),
		conn:    conn,
	}, nil
} // }}}

func Println(a ...any) (n int, err error) { // {{{
	if Debug {
		return fmt.Println(a...)
	}

	return
} // }}}
