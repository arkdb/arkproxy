# Arkproxy


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
- Percona 分支数据库支持无任何权限侵入直接上线的功能

  
  

[Arkproxy 介绍](http://www.cloud-ark.com/#/Arkproxy) | [ArkDB介绍](http://www.cloud-ark.com/#/ArkDB) | [关于极数云舟](http://www.cloud-ark.com/#/AboutUs)

  



## 安装

### 预编译的二进制安装

```
# 适用于 centos7/x86_64 系统
curl -sL -o /usr/local/bin/arkproxy https://github.com/arkdb/arkproxy/releases/download/v20.06.30/arkproxy
chmod +x /usr/local/bin/arkproxy
/usr/local/bin/arkproxy --version
```

### 从源码编译安装

```
# 以下环境为 centos7/x86_64 系统

# 1. 开发工具
yum install cmake git gcc gcc-c++ bison rpm-build \
            libxml2-devel libevent-devel ncurses-devel openssl-devel

# 2. 依赖库
git clone git@github.com:edenhill/librdkafka.git
cd librdkafka
./configure --install-deps --source-deps-only --disable-gssapi --disable-sasl \
  --disable-zstd --enable-static
make && make install

# 3. 编译 Arkproxy
git clone git@github.com:arkdb/arkproxy.git
cd arkproxy
./build.sh
install -v out/sql/arkproxy /usr/local/bin/arkproxy
```

## 使用


- 创建配置文件

  `curl -sL -o /etc/arkproxy.cnf https://raw.githubusercontent.com/arkdb/arkproxy/master/arkproxy.cnf.example`

  注：

  1) 请按需修改配置文件 `/etc/arkproxy.cnf`，根据实际环境情况替换 proxy_backend_user、proxy_backend_passwd、backend_host、backend_port 以及各节点的路由类型，可参考[详细的配置参数](https://github.com/arkdb/arkproxy/wiki/配置参数说明)

  2) 同一类型的路由，不能设置多次，比如 readonly，只能有一个配置区块，如果对应多个 Server，则可以将多个Server的名字写到 router_servers 中，逗号分隔放在一行
  3) **必须的配置项**
    - `proxy_backend_user` 和 `proxy_backend_passwd` , 用来连接后端 MySQL 数据库集群的用户名和密码，在后端数据库写节点 grant 授权
    - `proxy_shell_username` 和 `proxy_shell_password` , 在arkproxy.cnf 文件中配置用户和密码，通过其访问Arkproxy管理shell端口

- [构建 ArkDB 数据库集群](http://mirror.cloud-ark.com/public_package/ArkDB/极数云舟_云原生数据库ArkDB用户手册.pdf)或者使用已有的 MySQL 集群，并授权 Arkproxy，（附: ArkDB 的[下载](http://mirror.cloud-ark.com/public_package/ArkDB/ArkDB.tar.gz)和[安装](http://mirror.cloud-ark.com/public_package/ArkDB/极数云舟_云原生数据库ArkDB用户手册.pdf) ）

  登录数据库集群写节点执行：

```
GRANT ALL PRIVILEGES ON *.* TO 'arkproxy'@'127.0.0.1' IDENTIFIED BY 'arkproxy' WITH GRANT OPTION ;
GRANT ALL PRIVILEGES ON *.* TO 'arkproxy'@'%' IDENTIFIED BY 'arkproxy' WITH GRANT OPTION;
```

只需要授权配置文件中的 proxy_backend_user 和 proxy_backend_passwd 即可。而 proxy_shell_username && proxy_shell_password 是 Arkproxy 管理端口，不需要在写入节点进行授权。


- 启动 Arkproxy:

  `/usr/local/bin/arkproxy --defaults-file=/etc/arkproxy.cnf &`


- 业务访问 (DEV/应用程序)：

  `mysql -uarkproxy -parkproxy -P3336 -h127.0.0.1 -A`


- Arkproxy 管理(DBA/管理员):

  `mysql -uproxyshell -ppassword -P3335 -h127.0.0.1 -A`

  输入 `config help` 可看到支持的指令列表，[详细的命令说明](https://github.com/arkdb/arkproxy/wiki/管理端命令说明)




## 参与贡献
任何意见建议欢迎在 issues 中反馈，或者联系 opensource@cloud-ark.com

------

## AiDBA详解系列文章

[AiDBA详解系列（三）Arkproxy：兼容MySQL的轻量级中间件](https://mp.weixin.qq.com/s/sxhuA6QeSvvCSvxoyVM8PQ)

---

[AiDBA 详解系列（一）AiDBA 综述：做一个化解DBA身边种种难题的哆啦A梦](https://mp.weixin.qq.com/s/360CpgbelchLJkBnAM-jIw)

[AiDBA 详解系列（二）Arkwatch：MySQL 高可用解决方案集大成者](https://mp.weixin.qq.com/s/fB9Mnuk9azzlC8no5AuhDA)

[AiDBA 详解系列（三）Arkproxy：兼容MySQL的轻量级中间件](https://mp.weixin.qq.com/s/sxhuA6QeSvvCSvxoyVM8PQ)

[AiDBA 详解系列（四）企业业务数据整合利器-异构数据同步系统 Arkgate](https://mp.weixin.qq.com/s/I7kBrSW3TkLIZMF1iyETvA)

[AiDBA 详解系列（五）：数据库管理远程的手 Arkagent](https://mp.weixin.qq.com/s/VV4pkEH7zBYpOsj1Y8eHrA)

[AiDBA 详解系列（六）：数据库云管平台 Arkcontrol 功能概览](https://mp.weixin.qq.com/s/UN7Oe9572xR4tSN-kbGF5A)

[AiDBA 详解系列（七）：Arkcontrol 的服务层设计与实现](https://mp.weixin.qq.com/s/olFVGdaRblVYOOlPCW6b4Q)

[AiDBA 详解系列（八）：Arkcontrol 展示层设计与实现](https://mp.weixin.qq.com/s/KaeScj_jbNGq6qdRmBMXYw)

[AiDBA 详解系列（九）： Arkcontrol 数据存储的设计](https://mp.weixin.qq.com/s/i-Q7X8qGMOqV0XTW3CcM-Q)

[AiDBA 详解系列（十）MySQL 智能运维：DBA 身边的好帮手](https://mp.weixin.qq.com/s/eJk-MdvAU_Kf3eZyLGsc2w)

[AiDBA 详解系列（十一）： Arkcontrol 对 ArkDB 的运维支持](https://mp.weixin.qq.com/s/fkmU-1HJB_aUwB0MGG44FA)

[AiDBA 详解系列（十二）： Arkcontrol 对 Redis 的运维支持](https://mp.weixin.qq.com/s/F08NLA2HstX3oj-J2-R_8w)

[AiDBA 详解系列（十三）： Arkcontrol 对 Oracle 数据库的运维支持](https://mp.weixin.qq.com/s/bumUe6DGMmjDnzAaig0NCA)

[AiDBA 详解系列（十四）： Arkcontrol 对 MongoDB 的运维支持](https://mp.weixin.qq.com/s/QQR9Ga5HQShjIRohp8jnVA)

[AiDBA 详解系列（十五）： Arkcontrol 对 AiDBA 组件产品的运维支持](https://mp.weixin.qq.com/s/qETpcxdvpXbyvmb3b3OG0Q)

[AiDBA 详解系列（十六）SQL审核与执行：把风险 SQL 扼杀在摇篮](https://mp.weixin.qq.com/s/tPIgPcmrf9DIWr7Ir7AKZw)

[AiDBA 详解系列（十七）： Arkcontrol 智能 SQL 优化（推荐SQL索引）](https://mp.weixin.qq.com/s/Ej2epskeBaHciRWptBDpRA)

[AiDBA 详解系列（十八）： Arkcontrol 的数据库备份与恢复](https://mp.weixin.qq.com/s/xha-MHEvIGQsRbeqGfdc1Q)

[AiDBA 详解系列（十九）：针对 Arkcontrol 的配置管理](https://mp.weixin.qq.com/s/LQVDNKoDVUUQ4iFtCElxZg)

[AiDBA 详解系列（二十）：如何快速安装部署 Arkcontrol ](https://mp.weixin.qq.com/s/Jz-nDpbsI_AaZ-uJYOc7uA)

