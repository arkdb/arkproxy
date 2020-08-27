# Arkproxy

[Arkproxy 介绍](http://www.cloud-ark.com/#/Arkproxy) | [ArkDB介绍](http://www.cloud-ark.com/#/ArkDB) | [关于极数云舟](http://www.cloud-ark.com/#/AboutUs)



Arkproxy 是高性能、高可靠的数据库中间件，由极数云舟出品开源。




## 核心特性

- 透明读写分离和支持 Hint 分发
- 100%兼容MySQL语法，用户友好
- 自动负载均衡、权重分发，灵活控制数据库流量
- 内部实现消息压缩，同时实现用户连接数限制和统计
- Trace智能统计分析及审计，支持将访问请求对接到Kafka，供大数据系统统计分析
- 内置高效连接池，在高并发时大大提升数据库集群的处理能力
- 提供自定义一致性读和自路由一致性读来满足数据的强一致性读需求
- 自定义SQL 拦截，可以拦截危险SQL
- 配置文件可动态加载，避免重启

## 安装

### 预编译的二进制安装

```
# 适用于 centos7/x86_64 系统
curl -sL -o /usr/local/bin/arkproxy https://github.com/arkdb/arkproxy/tree/master/arkproxy.cnf.example
chmod +x /usr/local/bin/arkproxy
```

### 从源码编译安装

```
# 以下环境为 centos7/x86_64 系统

# 1. 开发工具
yum groupinstall 'development tools'
yum install cmake git gcc gcc-c++ bison rpm-build \
          libxml2-devel libevent-devel ncurses-devel ncurses-static openssl-devel

# 2. 依赖库
git clone git@github.com:edenhill/librdkafka.git
cd librdkafka
./configure --install-deps --source-deps-only  --disable-gssapi --disable-sasl --disable-zstd --enable-static  && make &&  make install

# 3. 编译 Arkproxy
git clone git@github.com:arkdb/arkproxy.git
cd arkproxy
./build.sh
install -v out/sql/arkproxy /usr/local/bin/arkproxy
```

## 使用


- 创建配置文件 `curl -sL -o /etc/arkproxy.cnf https://raw.githubusercontent.com/arkdb/arkproxy/master/arkproxy.cnf.example`

  注：

  1) 请按需修改配置文件 `/etc/arkproxy.cnf`，根据实际环境情况替换 proxy_backend_user、proxy_backend_passwd、backend_host、backend_port 以及各节点的路由类型，可参考详细的配置参数

  2) 同一类型的路由，不能设置多次，比如 readonly，只能有一个配置区块，如果对应多个 Server，则可以将多个Server的名字写到 router_servers 中，逗号分隔放在一行。

- 启动 Arkproxy `/usr/local/bin/arkproxy --defaults-file=/etc/arkproxy.cnf &`

- 在数据库写节点，授权 Arkproxy

  登录数据库集群写节点执行：

```
  GRANT ALL PRIVILEGES ON *.* TO 'arkproxy'@'127.0.0.1' IDENTIFIED BY 'arkproxy' WITH GRANT OPTION ;
  GRANT ALL PRIVILEGES ON *.* TO 'arkproxy'@'%' IDENTIFIED BY 'arkproxy' WITH GRANT OPTION;
```

  只需要授权配置文件中的 proxy_backend_user 和 proxy_backend_passwd 即可。而 proxy_shell_username && proxy_shell_password 是 Arkproxy 管理端口，不需要在写入节点进行授权。

- 业务访问：(DEV/应用程序)：`mysql -uarkproxy -parkproxy -P3336 -h10.0.0.134 -A`

- Arkproxy 管理(DBA/管理员): `mysql -uproxyshell -ppassword -P3335 -h10.0.0.134 -A`



## 参与贡献
任何意见建议欢迎在 issues 中反馈，或者联系 opensource@cloud-ark.com



## AiDBA详解系列文章

[AiDBA详解系列（三）Arkproxy：兼容MySQL的轻量级中间件](https://mp.weixin.qq.com/s/sxhuA6QeSvvCSvxoyVM8PQ)
