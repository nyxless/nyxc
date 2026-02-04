 # NyxClient
 ### version 1.0

 go client for NYX 

 ## Quick Start
	//使用默认配置
	c,err := nyxclient.NewNyxClient("127.0.0.1:9002", "appid", "secret")

	//设置grpc连接超时时间
	c,err := nyxclient.NewNyxClient("127.0.0.1:9002", "appid", "secret", grpc.WithTimeout(time.Duration(1) * time.Second))

	//设置连接池: 空闲连接数,  最大连接数, 
	c,err := nyxclient.NewNyxClient("127.0.0.1:9002", "appid", "secret", nyxclient.WithMaxIdleConns(5), nyxclient.WithMaxOpenConns(200))

	if err != nil {
		fmt.Printf("%#v\n", err)
		return
	}

	//无参数请求
	data, err := c.Request("user/GetUserInfo", nil)

	//有参数请求
	data, err := c.Request("user/GetUserInfo", map[string]interface{}{"uid": "123"})

	//设置请求超时时间
	data, err := c.Request("user/GetUserInfo", nil, nyxclient.WithTimeout(time.Duration(1) * time.Second))

	//设置缓存
	data, err := c.Request("user/GetUserInfo", nil, nyxclient.WithCache(300))

	//设置请求context
	data, err := c.Request("user/GetUserInfo", nil, nyxclient.WithContext(ctx))

	//设置请求header
	res, err := c.Request("user/GetUserInfo", nil, nyxclient.WithHeaders(map[string]string{"test-header":"header-value"}))

	if err != nil {
		fmt.Printf("%#v", err)
		return
	}

	fmt.Printf("code: %#v", res.GetCode())
	fmt.Printf("msg: %#v", res.GetMsg())
	fmt.Printf("data: %#v", res.GetData())
	fmt.Printf("header: %#v", res.GetHeaders())
	fmt.Printf("trailer: %#v", res.GetTrailers())
