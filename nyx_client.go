package nyxc

import (
	"context"
	"fmt"
	"github.com/nyxless/nyx/x"
	"github.com/nyxless/nyx/x/cache"
	"github.com/nyxless/nyx/x/log"
	"github.com/nyxless/nyx/x/pb"
	"github.com/nyxless/nyxc/rpc"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"sync"
	"time"
)

type NyxClient struct {
	Address string
	appid   string
	secret  string
	rpc     *rpc.RpcPool
	logger  *log.Logger
	cache   cache.LocalCache
	authFn  func(appid, secret string) map[string]string //鉴权函数, 根据服务端定义的校验算法，生成对应的header对
}

var (
	//缓存开关，需要Request时使用WithCache同时保持UseCache开关开启
	DefaultUseCache = false

	//缓存大小
	DefaultCacheSize = 100 * 1024 * 1024 //100M

	//默认GRPC连接超时时间3秒
	DefaultDialTimeout = time.Duration(3) * time.Second

	//默认请求超时时间3秒
	DefaultTimeout = time.Duration(3) * time.Second

	//默认流式请求超时时间60秒
	DefaultStreamTimeout = time.Duration(60) * time.Second

	//默认最大空闲连接数
	DefaultMaxIdleConns = 10

	//默认最大连接数
	DefaultMaxOpenConns = 2000

	//默认鉴权函数, 根据 appid, secret 生成 header 键值对, 以 header 形式发送到服务端校验
	DefaultAuthFn = func(appid, secret string) map[string]string {
		return map[string]string{
			"Appid":  appid,
			"Secret": secret,
		}
	}
)

var nyxclient_instance = map[string]*NyxClient{}
var mutex sync.RWMutex
var sf singleflight.Group

type CacheCallbackFn = rpc.CacheCallbackFn

// 设置参数MaxIdleConns, 支持方法: NewNyxClient
func WithMaxIdleConns(mic int) grpc.DialOption { // {{{
	return &rpc.DialOption{
		F: func(r *rpc.RpcOptions) {
			r.MaxIdleConns = mic
		},
	}
} // }}}

// 设置参数MaxOpenConns, 支持方法: NewNyxClient
func WithMaxOpenConns(moc int) grpc.DialOption { // {{{
	return &rpc.DialOption{
		F: func(r *rpc.RpcOptions) {
			r.MaxOpenConns = moc
		},
	}
} // }}}

// 设置参数AuthFn, 支持方法:NewNyxClient
func WithAuthFn(authFn func(appid, secret string) map[string]string) grpc.DialOption { // {{{
	return &rpc.DialOption{
		F: func(r *rpc.RpcOptions) {
			r.AuthFn = authFn
		},
	}
} // }}}

// 设置参数Debug, 支持方法:NewNyxClient
func Debug() grpc.DialOption { // {{{
	return &rpc.DialOption{
		F: func(r *rpc.RpcOptions) {
			r.Debug = true
		},
	}
} // }}}

// 设置参数UseCache, 支持方法:NewNyxClient
func UseCache(sizes ...int) grpc.DialOption { // {{{
	size := DefaultCacheSize
	if len(sizes) > 0 {
		size = sizes[0]
	}

	return &rpc.DialOption{
		F: func(r *rpc.RpcOptions) {
			r.UseCache = true
			r.CacheSize = size
		},
	}
} // }}}

// 设置参数 Timeout, 支持方法: Request, 表示请求超时时间 (若设置grpc连接超时时间, 在 NewNyxClient 中使用 grpc.WithTimeout)
func WithTimeout(timeout time.Duration) grpc.CallOption { // {{{
	return &rpc.CallOption{
		F: func(r *rpc.RpcOptions) {
			r.Timeout = timeout
		},
	}
} // }}}

// 设置参数Ctx, 支持方法: Request
func WithContext(ctx context.Context) grpc.CallOption { // {{{
	return &rpc.CallOption{
		F: func(r *rpc.RpcOptions) {
			r.Ctx = ctx
		},
	}
} // }}}

// 设置参数Headers, 支持方法: Request
func WithHeaders(headers map[string]string) grpc.CallOption { // {{{
	return &rpc.CallOption{
		F: func(r *rpc.RpcOptions) {
			r.Headers = headers
		},
	}
} // }}}

// 设置参数UseCache、CacheTtl、CacheCallback 支持方法: Request
func WithCache(ttl int, callbackFns ...CacheCallbackFn) grpc.CallOption { // {{{
	return &rpc.CallOption{
		F: func(r *rpc.RpcOptions) {
			r.UseCache = true
			r.CacheTtl = ttl
			if len(callbackFns) > 0 {
				r.CacheCallback = callbackFns[0]
			}
		},
	}
} // }}}

// 设置参数UseCache、CacheRefreshInterval、CacheCallback, 支持方法: Request
func WithRefreshCache(refreshInterval int, callbackFns ...CacheCallbackFn) grpc.CallOption { // {{{
	return &rpc.CallOption{
		F: func(r *rpc.RpcOptions) {
			r.UseCache = true
			r.CacheRefreshInterval = refreshInterval
			if len(callbackFns) > 0 {
				r.CacheCallback = callbackFns[0]
			}
		},
	}
} // }}}

func NewNyxClient(address, appid, secret string, opts ...grpc.DialOption) (*NyxClient, error) { //{{{
	mutex.RLock()
	ins, ok := nyxclient_instance[address]
	mutex.RUnlock()

	if !ok {
		mutex.Lock()
		defer mutex.Unlock()

		var err error
		ins, err = newNyxClient(address, appid, secret, opts...)
		if nil != err {
			return nil, err
		}

		nyxclient_instance[address] = ins
	}

	return ins, nil
} // }}}

func newNyxClient(address, appid, secret string, opts ...grpc.DialOption) (*NyxClient, error) { //{{{
	options := &rpc.RpcOptions{
		MaxIdleConns: DefaultMaxIdleConns,
		MaxOpenConns: DefaultMaxOpenConns,
		UseCache:     DefaultUseCache,
		CacheSize:    DefaultCacheSize,
		Debug:        false,
		AuthFn:       DefaultAuthFn,
	}

	grpc_opts := []grpc.DialOption{
		grpc.WithTimeout(DefaultDialTimeout),
	}

	for _, opt := range opts {
		if o, ok := opt.(*rpc.DialOption); ok {
			o.F(options)
		} else {
			grpc_opts = append(grpc_opts, opt)
		}
	}

	if options.MaxIdleConns <= 0 {
		options.MaxIdleConns = 1
	}

	if options.MaxOpenConns <= 0 {
		options.MaxOpenConns = 1
	}

	if options.MaxIdleConns > options.MaxOpenConns {
		options.MaxIdleConns = options.MaxOpenConns
	}

	var rpclient *rpc.RpcPool
	var logger *log.Logger
	var err error

	if x.Logger != nil { //如果在nyx框架项目中，直接使用x.logger的配置
		logger = x.Logger
	} else {
		logger, err = getLogger()
		if err != nil {
			return nil, err
		}
	}

	rpclient, err = rpc.New(address, options.MaxIdleConns, options.MaxOpenConns, grpc_opts)
	if err != nil {
		logger.Log("nyxclient", err)
		return nil, err
	}

	if options.Debug {
		rpc.Debug = true
	}

	nyxclient := &NyxClient{
		Address: address,
		appid:   appid,
		secret:  secret,
		rpc:     rpclient,
		logger:  logger,
		authFn:  options.AuthFn,
	}

	if options.UseCache {
		if x.LocalCache != nil {
			nyxclient.cache = x.LocalCache
			x.Info("nyxclient use x.LocalCache")
		} else {
			nyxclient.cache = cache.NewLocalCache(options.CacheSize)
			x.Info("nyxclient LocalCache Init, size:", options.CacheSize)
		}

		nyxclient.cache.WithLogger(logger)
	}

	fmt.Println("nyxclient init:", address)

	return nyxclient, nil
} // }}}

func getLogger() (*log.Logger, error) { // {{{
	logger, err := log.NewLogger()
	if err != nil {
		return nil, err
	}

	logger.SetLevel(log.LevelCustom)

	fwriter, err := log.NewFileWriter("logs", "nyxclient-2006-01-02.log")
	if err != nil {
		return nil, err
	}

	fwriter_err, err := log.NewFileWriter("logs", "nyxclient-err-2006-01-02.log")
	if err != nil {
		return nil, err
	}

	logger.AddWriter(fwriter, "nyxclient")         //使用自定义日志名:nyxclient
	logger.AddWriter(fwriter_err, "nyxclient-err") //使用自定义日志名:nyxclient

	return logger, nil
} // }}}

func (this *NyxClient) Request(method string, params any, opts ...grpc.CallOption) (*rpc.Response, error) { // {{{
	ctx, cancel, options, grpc_opts := this.prepare(false, opts)
	defer cancel()

	ctx = context.WithValue(ctx, "method", method)
	ctx = context.WithValue(ctx, "params", params)

	var data map[string]any
	var reply *pb.Reply
	var header, trailer metadata.MD
	var err error

	if params != nil {
		data = x.AsMap(params)
	}

	if options.UseCache && this.cache != nil {
		cache_data, hit, err := this.getFromCache(method, data, options.CacheTtl, options.CacheRefreshInterval, options.CacheCallback, opts)
		if nil != err {
			this.logger.Log("nyxclient", err, map[string]any{"appid": this.appid, "uri": method, "options": options}, params)
			return nil, err
		}

		reply = cache_data.Reply
		header = cache_data.Header
		trailer = cache_data.Trailer
		ctx = context.WithValue(ctx, "hit_cache", hit)
		ctx = context.WithValue(ctx, "cache_time", cache_data.CacheTime)

	} else {
		grpc_opts = append(grpc_opts, grpc.Header(&header), grpc.Trailer(&trailer))
		reply, err = this.rpc.Request(ctx, method, data, grpc_opts)
		if nil != err {
			this.logger.Log("nyxclient", err, map[string]any{"appid": this.appid, "uri": method, "options": options}, data)
			return nil, err
		}
	}

	return rpc.NewResponse(ctx, reply, header, trailer, this.logger), nil
} //}}}

func (this *NyxClient) RequestStream(method string, params any, opts ...grpc.CallOption) (*rpc.ResponseIter, error) { // {{{
	ctx, cancel, options, grpc_opts := this.prepare(true, opts)

	ctx = context.WithValue(ctx, "method", method)
	ctx = context.WithValue(ctx, "params", params)

	var data map[string]any
	var header, trailer metadata.MD

	if params != nil {
		data = x.AsMap(params)
	}

	grpc_opts = append(grpc_opts, grpc.Header(&header), grpc.Trailer(&trailer))
	stream, err := this.rpc.RequestStream(ctx, method, data, grpc_opts)
	if nil != err {
		this.logger.Log("nyxclient", err, map[string]any{"appid": this.appid, "uri": method, "options": options}, params)
		return nil, err
	}

	return rpc.NewResponseIter(ctx, cancel, stream, header, trailer, this.logger), nil
} //}}}

func (this *NyxClient) prepare(is_stream bool, opts []grpc.CallOption) (ctx context.Context, cancel context.CancelFunc, options *rpc.RpcOptions, grpc_opts []grpc.CallOption) { // {{{
	timeout := DefaultTimeout
	if is_stream {
		timeout = DefaultStreamTimeout
	}

	options = &rpc.RpcOptions{
		Timeout: timeout,
		Headers: map[string]string{},
	}

	grpc_opts = []grpc.CallOption{}
	for _, opt := range opts {
		if o, ok := opt.(*rpc.CallOption); ok {
			o.F(options)
		} else {
			grpc_opts = append(grpc_opts, opt)
		}
	}

	//生成token放入header，在服务端校验
	auth_headers := this.authFn(this.appid, this.secret)
	for k, v := range auth_headers {
		options.Headers[k] = v
	}

	if options.Ctx != nil {
		ctx = options.Ctx

		if v := ctx.Value("guid"); v != nil {
			if guid, ok := v.(string); ok {
				options.Headers["guid"] = guid
			}
		}
	} else {
		ctx = context.Background()
	}

	ctx = context.WithValue(ctx, "appid", this.appid)

	if options.Headers["guid"] == "" {
		options.Headers["guid"] = x.GetUUID()
	}

	if options.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, options.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	md := metadata.MD{}
	for k, v := range options.Headers {
		md.Append(k, v)
	}

	ctx = metadata.NewOutgoingContext(ctx, md)

	return
} //}}}

func (this *NyxClient) Close() {
	this.logger.Close()
	this.cache.Clear()
}

func (this *NyxClient) getFromCache(method string, data map[string]any, ttl, refreshInterval int, callbackFn CacheCallbackFn, opts []grpc.CallOption) (*cacheData, bool, error) { // {{{
	fn := func() ([]byte, bool, error) {
		ctx, cancel, options, grpc_opts := this.prepare(false, opts)
		defer cancel()

		var header, trailer metadata.MD
		grpc_opts = append(grpc_opts, grpc.Header(&header), grpc.Trailer(&trailer))

		reply, err := this.rpc.Request(ctx, method, data, grpc_opts)
		if nil != err {
			this.logger.Log("nyxclient", err, map[string]any{"appid": this.appid, "uri": method, "options": options}, data)
			return nil, false, err
		}

		cache_data, err := x.GobEncode(&cacheData{reply, header, trailer, x.NowTime()})
		if nil != err {
			this.logger.Log("nyxclient", err, map[string]any{"appid": this.appid, "uri": method, "options": options}, data)
			return nil, false, err
		}

		return cache_data, reply.Code == 0, nil
	}

	key := this.getCacheKey(method, data)

	t := ttl
	cacheGetFn := this.cache.GetOrSetFn
	if refreshInterval > 0 {
		t = refreshInterval
		cacheGetFn = this.cache.GetOrRefreshFn
	}

	var baseCacheCallbackFn func([]byte) error
	if callbackFn != nil {
		baseCacheCallbackFn = func(data []byte) error {
			var cache_data *cacheData
			err := x.GobDecode(data, &cache_data)
			if err != nil {
				return err
			}

			return callbackFn(rpc.Process(cache_data.Reply.Data))
		}
	}

	got, hit, err := cacheGetFn(key, fn, t, baseCacheCallbackFn)

	if err != nil {
		return nil, false, err
	}

	var cache_data *cacheData
	err = x.GobDecode(got, &cache_data)
	if err != nil {
		return nil, false, err
	}

	return cache_data, hit, nil
} // }}}

func (this *NyxClient) getCacheKey(method string, data map[string]any) []byte {
	return []byte(method + x.MapToString(data))
}

type cacheData struct {
	Reply     *pb.Reply
	Header    metadata.MD
	Trailer   metadata.MD
	CacheTime time.Time
}
