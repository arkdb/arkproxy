//#include "my_default.h"
#include "my_getopt.h"
#include <mysql.h>
#include "sql_class.h"
#include <sys/types.h>
#include <ctype.h>
#include <string.h>
#include <sys/types.h>
#include <regex.h>
#include <string.h>
#include "sql_acl.h"
#include "sql_rules.h"
#include "ark_config.h"

THD* config_start_thd = NULL;


char server_version_space[SERVER_VERSION_LENGTH];
char* backend_server_version = server_version_space;
char *config_type;
char *backend_host;
long backend_port;
char *server_router_type;
char *router_servers;
char *listen_type;
long listen_port;
char *proxy_comments;
long server_weight;
long server_max_slave_lag;
char* server_status;
// char *shell_username;
// char *shell_password;

#if __CONSISTEND_READ__
consistend_cache_t global_consistend_cache;
#endif
extern trace_cache_t* global_trace_cache;
extern timeout_cache_t* global_timeout_cache;
extern char* config_groups[MAX_CONFIG_GROUP];
proxy_config_t global_proxy_config;
int proxy_create_user_config();
int proxy_load_users();
int proxy_load_rules();
int proxy_create_rules_config();
MYSQL* get_backend_connection(THD* thd, backend_conn_t* conn);
bool setup_connection_thread_globals(THD *thd);
MYSQL* proxy_get_config_server_connection(int need_lock);

#define PROXY_TYPE_SERVER "server"
#define PROXY_TYPE_ROUTER "router"
#define PROXY_TYPE_LISTEN "listen"

#define PROXY_TYPE "proxy_type"
#define CONFIG_COMMENT "config_comment"
#define BACKEND_HOST "backend_host"
#define BACKEND_PORT "backend_port"
#define SERVER_WEIGHT "weight"
#define SERVER_SLAVE_MAX_LAG "max_slave_lag"
#define SERVER_STATUS "server_status"
#define ROUTER_TYPE "router_type"
#define ROUTER_SERVERS "router_servers"
#define LISTEN_TYPE "listen_type"
#define LISTEN_PORT "listen_port"

#define SHELL_USERNAME "shell_username"
#define SHELL_PASSWORD "shell_password"

#define LISTEN_PROXY "proxy"
#define LISTEN_SHELL "shell"

#define ROUTER_READONLY "readonly"
#define ROUTER_READWRITE "readwrite"

struct my_option proxy_config_options[]=
{
  /* common variables */
  {PROXY_TYPE, 0, "server, router, Mlisten, or others",
   &config_type, &config_type, 0, GET_STR,
   REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  {CONFIG_COMMENT, 0, "comments ",
   &proxy_comments, &proxy_comments, 0, GET_STR,
   REQUIRED_ARG, 0, 0, 0, 0, 0, 0},

  /* server variables, include ip, port, and others */
  {BACKEND_HOST, 0, "backend server ip",
   &backend_host, &backend_host, 0, GET_STR,
   REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  {BACKEND_PORT, 0, "backend server port.",
   &backend_port, &backend_port, 0, GET_ULONG,
   REQUIRED_ARG, 3306, 0, 0, 0, 0, 0},
  {SERVER_WEIGHT, 0, "backend server weight.",
   &server_weight, &server_weight, 0, GET_ULONG,
   REQUIRED_ARG, 1, 0, 100, 0, 0, 0},
  {SERVER_SLAVE_MAX_LAG, 0, "max slave lag can routed to .",
   &server_max_slave_lag, &server_max_slave_lag, 0, GET_ULONG,
   REQUIRED_ARG, 1000, 0, UINT_MAX, 0, 0, 0},
  {SERVER_STATUS, 0, "server status, include offline, online.",
   &server_status, &server_status, 0, GET_STR,
   REQUIRED_ARG, 0, 0, 0, 0, 0, 0},

  /* router variables, include server names others */
  {ROUTER_TYPE, 0, "the router type, include readwrite, readonly",
   &server_router_type, &server_router_type, 0, GET_STR,
   REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  {ROUTER_SERVERS, 0, "the router target, the servers",
   &router_servers, &router_servers, 0, GET_STR,
   REQUIRED_ARG, 0, 0, 0, 0, 0, 0},

  /* listen variables, include ip, port, and others */
  {LISTEN_TYPE, 0, "the listen, include server connections, and mangentment connections",
   &listen_type, &listen_type, 0, GET_STR,
   REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  {LISTEN_PORT, 0, "listen port.",
   &listen_port, &listen_port, 0, GET_ULONG,
   REQUIRED_ARG, 3306, 0, 0, 0, 0, 0},
  // {SHELL_USERNAME, 0, "shell user name",
  //  &shell_username, &shell_username, 0, GET_STR,
  //  REQUIRED_ARG, 0, 0, 0, 0, 0, 0},
  // {SHELL_PASSWORD, 0, "shell password",
  //  &shell_password, &shell_password, 0, GET_STR,
  //  REQUIRED_ARG, 0, 0, 0, 0, 0, 0},

  {0, 0, 0, 0, 0, 0, GET_NO_ARG, NO_ARG, 0, 0, 0, 0, 0, 0}

};

int trace_metadata_not_local()
{
    if (proxy_trace_storage_host && proxy_trace_storage_port && 
        proxy_trace_storage_user && proxy_trace_storage_passwd && proxy_namespace &&
        !(strlen(proxy_trace_storage_host) == 0 || proxy_trace_storage_port == 0 ||
        strlen(proxy_trace_storage_user) == 0 || strlen(proxy_trace_storage_passwd) == 0 || 
        strlen(proxy_namespace) == 0 || strlen(proxy_namespace) >= NAME_CHAR_LEN))
    {
        return true;
    }

    return false;
}

int metadata_not_local()
{
    if (proxy_config_host && proxy_config_port && 
        proxy_config_user && proxy_config_passwd && proxy_namespace &&
        !(strlen(proxy_config_host) == 0 || proxy_config_port == 0 ||
        strlen(proxy_config_user) == 0 || strlen(proxy_config_passwd) == 0 || 
        strlen(proxy_namespace) == 0 || strlen(proxy_namespace) >= NAME_CHAR_LEN))
    {
        return true;
    }

    return false;
}

//strtok非线程安全，strtok_r是线程安全的
int split_chars(char dst[][100], char* str, const char* spl)
{
    int n = 0;
    char *p[100];
    char *last = NULL;
    
    while((p[n]=strtok_r(str,spl,&last))!=NULL)
    {
        strcpy(dst[n], p[n]);
        n++;
        str=NULL;
    }
    
    return n;
}

void trim(char *src)    // 删除C风格字符串中的空格
{
    int len_dst=0;
    int len_src=0;
    char * fp = src;
    while (*src) {
        if (!isspace(*src)) { // 如果不是空格就复制
            *fp = *src;
            fp++;
            len_dst++;
        }
        src++;
        len_src++;
    }

    while (len_dst <= len_src)
    {
        *fp = '\0' ; //封闭字符串
        fp++;
        len_dst++;
    }
}

void proxy_reset_config()
{
    // my_free(config_type);
    config_type = NULL;
    backend_port = 0;
    // my_free(backend_host);
    backend_host = NULL;
    // my_free(server_router_type);
    server_router_type = NULL;
    // my_free(router_servers);
    router_servers = NULL;
    // my_free(listen_type);
    listen_type = NULL;
    listen_port = 0;
    // my_free(proxy_comments);
    proxy_comments = NULL;
    server_status = NULL;
    server_weight = 0;
    server_max_slave_lag = 0;
}

int get_local_IP(char* to)
{
    char msg[1024] = {0};
    MYSQL* mysql = NULL;
    ulong client_flag= CLIENT_REMEMBER_OPTIONS;
    uint net_timeout= 10;
    uint connect_timeout= 10;
    
    mysql = mysql_init(mysql);
    mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, (char *) &connect_timeout);
    mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, (char *) &net_timeout);
    mysql_options(mysql, MYSQL_OPT_WRITE_TIMEOUT, (char *) &net_timeout);
    mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "utf8mb4");
    // mysql_options(mysql, MYSQL_SET_CHARSET_DIR, (char *) charsets_dir);

    proxy_server_t* server = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
    if (mysql_real_connect(mysql, server->server->backend_host, backend_user,
                           "1", NULL, server->server->backend_port, NULL,
                           client_flag) == 0)
    {
        if (mysql_errno(mysql) != 1045)
        {
            sql_print_error("Could not get local ip, error: %s", mysql_error(mysql));
            mysql_close(mysql);
            return true;
        }
        strcpy(msg, mysql_error(mysql));
        char s_arr_1[10][100];
        memset(s_arr_1, 0, 10*100);
        split_chars(s_arr_1, msg, "\'");
        strcpy(to, s_arr_1[3]);
        if (strcasecmp(to, "localhost") == 0)
        {
            strcpy(to, "127.0.0.1");
        }
        mysql_close(mysql);
        return false;
    }
    mysql_close(mysql);

    return true;
}

int proxy_set_config(char* group)
{
    proxy_servers_t* server;
    proxy_servers_t* server_new;
    proxy_router_t* router;
    proxy_listens_t* listen;

    if (config_type && !strcasecmp(PROXY_TYPE_SERVER, config_type))
    {
        if (backend_host && strlen(backend_host) > 0 &&
            backend_port > 0)
        {
            server = (proxy_servers_t*)my_malloc(sizeof(proxy_servers_t), MY_ZEROFILL);
            strcpy(server->server_name, group);
            strcpy(server->backend_host, backend_host);
            server->backend_port = backend_port;
            server->weight = server_weight;
            server->current_weight = server_weight;
            server->max_slave_lag = server_max_slave_lag;
            if (server_status)
            {
                if (!strcasecmp(server_status, "OFFLINE"))
                    server->server_status = SERVER_STATUS_OFFLINE;
                else if (!strcasecmp(server_status, "ONLINE"))
                    server->server_status = SERVER_STATUS_ONLINE;
                else
                {
                    sql_print_information(ER_DEFAULT(ER_CONFIG_INVALID), 
                        SERVER_STATUS, server_status);
                    return true;
                }
            }
            else
                server->server_status = SERVER_STATUS_ONLINE;

            if (proxy_comments)
            {
                server->comments = (char*)my_malloc(strlen(proxy_comments)+1, MY_ZEROFILL);
                strcpy(server->comments, proxy_comments);
            }

//            server_new = LIST_GET_FIRST(global_proxy_config.server_lst);
//            while (server_new)
//            {
//                if (!strcasecmp(server_new->backend_host, server->backend_host) &&
//                    server_new->backend_port == server->backend_port)
//                {
//                    sql_print_error("there have at least two server with "
//                        "host: %s, port: %d", server->backend_host, server->backend_port);
//                    return true;
//                }
//
//                server_new = LIST_GET_NEXT(link, server_new);
//            }

            LIST_ADD_LAST(link, global_proxy_config.server_lst, server);
        }
    }
    else if (config_type && !strcasecmp(PROXY_TYPE_ROUTER, config_type))
    {
        if (server_router_type && strlen(server_router_type) > 0 &&
            router_servers && strlen(router_servers) > 0)
        {
            router = (proxy_router_t*)my_malloc(sizeof(proxy_router_t), MY_ZEROFILL);
            router->servers = (char*)my_malloc(strlen(router_servers)+1, MY_ZEROFILL);
            strcpy(router->servers, router_servers);
            router->router_type = strcasecmp(ROUTER_READONLY, server_router_type) 
              == 0 ? ROUTER_TYPE_RO : ROUTER_TYPE_RW;
            if (router->router_type == ROUTER_TYPE_RW && 
                strcasecmp(ROUTER_READWRITE, server_router_type))
            {
                sql_print_information(ER_DEFAULT(ER_CONFIG_INVALID), 
                    ROUTER_TYPE, server_router_type);
                return true;
            }
                
            if (proxy_comments)
            {
                router->comments = (char*)my_malloc(strlen(proxy_comments)+1, MY_ZEROFILL);
                strcpy(router->comments, proxy_comments);
            }
            strcpy(router->router_name, group);
            LIST_ADD_LAST(link, global_proxy_config.router_lst, router);
        }
    }
    else if (config_type && !strcasecmp(PROXY_TYPE_LISTEN, config_type))
    {
        if (listen_type && strlen(listen_type) > 0 &&
            listen_port > 0)
        {
            listen = (proxy_listens_t*)my_malloc(sizeof(proxy_listens_t), MY_ZEROFILL);
            listen->listen_type = strcasecmp(LISTEN_PROXY, listen_type) 
              == 0 ? LISTEN_TYPE_PROXY : LISTEN_TYPE_SHELL;
            if (listen->listen_type == LISTEN_TYPE_SHELL && strcasecmp(LISTEN_SHELL, listen_type))
            {
                sql_print_information(ER_DEFAULT(ER_CONFIG_INVALID), 
                    LISTEN_TYPE, listen_type);
                return true;
            }
                
            if (proxy_comments)
            {
                listen->comments = (char*)my_malloc(strlen(proxy_comments)+1, MY_ZEROFILL);
                strcpy(listen->comments, proxy_comments);
            }
            LIST_ADD_LAST(link, global_proxy_config.listen_lst, listen);
        }
    }
    else 
    {
        sql_print_information(ER_DEFAULT(ER_CONFIG_INVALID), 
            PROXY_TYPE, config_type);
        return true;
    }

    return false;
}

/* WARINGS: 
 * must in the lock global_proxy_config.config_lock */
proxy_servers_t* find_server_by_name(char* server_name)
{
    proxy_servers_t* server = LIST_GET_FIRST(global_proxy_config.server_lst);
    while (server)
    {
        if (strcmp(server_name, server->server_name) == 0)
            return server;
        server = LIST_GET_NEXT(link, server);
    }
    return NULL;
}

//strtok非线程安全，多线程请用strtok_r
bool add_to_server_list(char* servers, int type, proxy_router_t* route)
{
    int count = 0;
    proxy_servers_t* server;
    char *server_str = NULL;
    server_str = strtok(servers, ",");
    while (server_str != NULL)
    {
        trim(server_str);
        server = find_server_by_name(server_str);
        if (server)
        {
            proxy_server_t* tmp_server =
                (proxy_server_t*)my_malloc(sizeof(proxy_server_t), MY_ZEROFILL);
            tmp_server->route = route;
            tmp_server->server = server;
            if (type == ROUTER_TYPE_RW) {
              LIST_ADD_LAST(link, global_proxy_config.rw_server_lst,
                            tmp_server);
              if (server->server_status == SERVER_STATUS_ONLINE)
                count++;
              server->routed |= ROUTED_TYPE::WRITE_ROUTED;
            } else {
              LIST_ADD_LAST(link, global_proxy_config.ro_server_lst,
                            tmp_server);
              server->routed |= ROUTED_TYPE::READ_ROUTED;
            }
//            if (route->comments)
//            {
//                tmp_server->route->comments = 
//                  (char*)my_malloc(strlen(route->comments)+1, MY_ZEROFILL);
//                strcpy(tmp_server->route->comments, route->comments);
//            }
            tmp_server->route->router_type = route->router_type;
        }
        else
        {
            sql_print_error("Got wrong server name: %s.", server_str);
            return true;
        }
        
        server_str = strtok(NULL, ",");
    }

    if (!proxy_multi_write_mode && count > 1)
    {
        sql_print_error("Got more than one server when proxy_multi_write_mode=false.");
        return true;
    }
    
    /* if there is no write node */
    if (LIST_GET_LEN(global_proxy_config.rw_server_lst) == 0)
    {
        sql_print_error("You need to configure at least one write node");
        return true;
    }

    return false;
}

bool split_server_list()
{
    char* server;
    proxy_router_t* router = LIST_GET_FIRST(global_proxy_config.router_lst);
    while (router)
    {
        server = (char*)my_malloc(strlen(router->servers)+1, MY_ZEROFILL);
        strcpy(server, router->servers);
        if (add_to_server_list(server, router->router_type, router))
        {
            my_free(server);
            return true;
        }
        my_free(server);

        router = LIST_GET_NEXT(link, router);
    }

    return false;
}

//router type的唯一性检查
bool check_router_config()
{
    proxy_router_t* router = LIST_GET_FIRST(global_proxy_config.router_lst);
    while (router)
    {
        int count = 0;
        proxy_router_t* head = LIST_GET_FIRST(global_proxy_config.router_lst);
        while (head)
        {
            if (router->router_type == head->router_type)
                count++;
            head = LIST_GET_NEXT(link, head);
        }
        if (count != 1)
            return true;
        router = LIST_GET_NEXT(link, router);
    }

    return false;
}

bool invalid_ip(char *buf)
{
    int status;
    int cflags=REG_EXTENDED;
    regmatch_t pmatch[1];
    const size_t nmatch=1;
    regex_t reg;
    const char *pattern="^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9]).){3}(25[0-5]|2[0-5][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])$";
    
    regcomp(&reg,pattern,cflags);
    status=regexec(&reg,buf,nmatch,pmatch,0);
    regfree(&reg);
    if(status==REG_NOMATCH)
        return true;
    else if(status==0){
        return false;
    }
    return false;
}

bool check_three_point(char *ip)
{
    if (ip != NULL)
    {
        int in = 0;
        char *p[20] = {0};
        char *inner_ptr = NULL;
        char s[16]  = {0};
        strcpy(s, ip);
        char* buf = s;
        
        while ((p[in] = strtok_r(buf, ".", &inner_ptr)) != NULL)
        {
            in++;
            buf = NULL;
        }

        if (in == 4)
            return false;
        else
            return true;
    }
    else
        return true;
}

bool check_ip(char* ips)
{
    if (ips == NULL)
        return true;
    int in = 0;
    char *p[20] = {0};
    char *inner_ptr = NULL;
    char *buf;
    char *buf_free;

    buf = (char*)my_malloc(10 + strlen(ips), MY_ZEROFILL);
    strcpy(buf, ips);
    buf_free = buf;
    
    while ((p[in] = strtok_r(buf, ",", &inner_ptr)) != NULL)
    {
        if (strlen(p[in]) > 15
            || check_three_point(p[in])
            || invalid_ip(p[in]))
        {
            sql_print_error("Invalid white ip: %s", p[in]);
            my_free(buf_free);
            return true;
        }

        in++;
        buf = NULL;
    }

    my_free(buf_free);
    return false;
}

//strtok非线程安全，多线程请用strtok_r
bool split_white_ips()
{
    char *ip_str = NULL;
    char *buf;

    if (proxy_white_ips == NULL)
        return false;

    buf = (char*)my_malloc(10 + strlen(proxy_white_ips), MY_ZEROFILL);
    strcpy(buf, proxy_white_ips);

    ip_str = strtok(buf, ",");
    while (ip_str != NULL)
    {
        trim(ip_str);
        if (strlen(ip_str) > 15)
        {
            ip_str = strtok(NULL, ",");
            continue;
        }

        proxy_white_ip_t* white_ip =
            (proxy_white_ip_t*)my_malloc(sizeof(proxy_white_ip_t), MY_ZEROFILL);
        memset(white_ip->ip, 0, 15);
        strcpy(white_ip->ip, ip_str);
        ip_str = strtok(NULL, ",");
        LIST_ADD_LAST(link, global_proxy_config.white_ip_lst, white_ip);
    }

    my_free(buf);

    return false;
}

bool check_incoming_addresses_low(char* ips)
{
    if (ips == NULL)
        return true;
    int in = 0;
    char *p[20] = {0};
    char *inner_ptr = NULL;
    char *buf;
    char *buf_free;
    address_t* addr;
    int ret;
    int count = 0;

    buf = (char*)my_malloc(10 + strlen(ips), MY_ZEROFILL);
    strcpy(buf, ips);
    buf_free = buf;
    
    while ((p[in] = strtok_r(buf, ",", &inner_ptr)) != NULL)
    {
        addr = (address_t*)my_malloc(sizeof(address_t), MY_ZEROFILL);
        ret = sscanf(p[in], "%[^:]:%ld", addr->hostname, &addr->port);
        if (ret != 2)
        {
            my_error(ER_IP_INCORRECT, MYF(0));
            my_free(addr);
            return true;
        }

        if (++count > 32)
        {
            my_error(ER_TOO_MANY_CLUSTER_NODE, MYF(0), 32);
            return true;
        }

        my_free(addr);
        in++;
        buf = NULL;
    }

    my_free(buf_free);
    return false;
}

bool update_incoming_addresses_low()
{
    char *buf;
    address_t* addr;
    int in = 0;
    char *p[20] = {0};
    char *inner_ptr = NULL;

    if (proxy_incoming_addresses[0] == '\0')
        return false;

    buf = (char*)my_malloc(10 + strlen(proxy_incoming_addresses), MY_ZEROFILL);
    strcpy(buf, proxy_incoming_addresses);

    while (LIST_GET_LEN(global_proxy_config.incoming_lst))
    {
        addr = LIST_GET_FIRST(global_proxy_config.incoming_lst);
        LIST_REMOVE(link, global_proxy_config.incoming_lst, addr);
        my_free(addr);
    }

    while ((p[in] = strtok_r(buf, ",", &inner_ptr)) != NULL)
    {
        trim(p[in]);
        addr = (address_t*)my_malloc(sizeof(address_t), MY_ZEROFILL);
        sscanf(p[in], "%[^:]:%ld", addr->hostname, &addr->port);
        LIST_ADD_LAST(link, global_proxy_config.incoming_lst, addr);
        in++;
        buf = NULL;

        if (LIST_GET_LEN(global_proxy_config.incoming_lst) > 32)
        {
            my_free(buf);
            return true;
        }
    }

    my_free(buf);

    return false;
}

int proxy_check_config()
{
    //先做检查，不符合规则就返回
    if (check_router_config())
    {
        sql_print_error("Duplicate router type, please check.");
        return true;
    }

    //下面的执行都是在符合条件下，所以可以对参数合法性忽略
    if (split_server_list())
    {
        sql_print_error("Go wrong server name, please check.");
        return true;
    }
    
    if (check_ip(proxy_white_ips) || split_white_ips())
    {
        sql_print_error("Got wrong value of proxy_non_encrypted_ips, please check.");
        return true;
    }

    if (backend_user == NULL || backend_passwd == NULL || 
        backend_user[0] == '\0' || backend_passwd[0] == '\0')
    {
        sql_print_error("Got wrong value of proxy_backend_user/proxy_backend_passwd, please check.");
        return true;
    }

    return false;
}

int proxy_config_deinit()
{
    /* TODO: 清内存 */
    proxy_white_ip_t* white_ip;
    proxy_white_ip_t* white_ip_next;
    proxy_router_t* routes;
    proxy_server_t* server;
    proxy_servers_t* servers;
    proxy_router_t* routes_next;
    proxy_server_t* server_next;
    proxy_servers_t* servers_next;
    proxy_listens_t* listen;
    proxy_listens_t* listen_next;
    
    routes = LIST_GET_FIRST(global_proxy_config.router_lst);
    while (routes)
    {
        routes_next = LIST_GET_NEXT(link, routes);
        LIST_REMOVE(link, global_proxy_config.router_lst, routes);
        if (routes->comments)
            my_free(routes->comments);
        if (routes->servers)
            my_free(routes->servers);
        my_free(routes);

        routes = routes_next;
    }

    listen = LIST_GET_FIRST(global_proxy_config.listen_lst);
    while (listen)
    {
        listen_next = LIST_GET_NEXT(link, listen);
        LIST_REMOVE(link, global_proxy_config.listen_lst, listen);
        if (listen->comments)
            my_free(listen->comments);
        my_free(listen);

        listen = listen_next;
    }

    server = LIST_GET_FIRST(global_proxy_config.ro_server_lst);
    while (server)
    {
        server_next = LIST_GET_NEXT(link, server);
        LIST_REMOVE(link, global_proxy_config.ro_server_lst, server);
        my_free(server);

        server = server_next;
    }

    white_ip = LIST_GET_FIRST(global_proxy_config.white_ip_lst);
    while (white_ip)
    {
        white_ip_next = LIST_GET_NEXT(link, white_ip);
        LIST_REMOVE(link, global_proxy_config.white_ip_lst, white_ip);
        my_free(white_ip);

        white_ip = white_ip_next;
    }

    server = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
    while (server)
    {
        server_next = LIST_GET_NEXT(link, server);
        LIST_REMOVE(link, global_proxy_config.rw_server_lst, server);
        my_free(server);

        server = server_next;
    }

    servers = LIST_GET_FIRST(global_proxy_config.server_lst);
    while (servers)
    {
        servers_next = LIST_GET_NEXT(link, servers);
        LIST_REMOVE(link, global_proxy_config.server_lst, servers);
        if (servers->comments)
            my_free(servers->comments);
        my_free(servers);

        servers = servers_next;
    }

    my_hash_free(&global_proxy_config.proxy_user);
    return false;
}

int proxy_config_thd_deinit()
{
    if (config_start_thd)
    {
        delete config_start_thd;
        config_start_thd = NULL;
    }
    return false;
}

int proxy_config_thd_init()
{
    my_thread_init();
    config_start_thd = new THD(0);
    config_start_thd->thread_stack= (char*) &config_start_thd;
    setup_connection_thread_globals(config_start_thd);

    backend_conn_t* conn = get_cluster_fixed_conn(config_start_thd);
    if (conn == NULL)
        return true;
    strcpy(backend_server_version, ((MYSQL*)conn->get_mysql())->server_version);
    close_backend_connection(conn);
    return false;
}

int proxy_config_init( int *argc, char ***argv, char** groups)
{
    int err=0;
    mysql_mutex_init(NULL, &global_proxy_config.config_lock, MY_MUTEX_INIT_FAST);
    //TODO: deinit    
    mysql_rwlock_init(key_rwlock_global_config, &global_proxy_config.config_rwlock);

    mysql_mutex_init(NULL, &global_proxy_config.white_ip_lock, MY_MUTEX_INIT_FAST);
    mysql_mutex_init(NULL, &global_proxy_config.current_weight_lock, MY_MUTEX_INIT_FAST);
    mysql_mutex_init(NULL, &global_trace_cache->trace_lock, MY_MUTEX_INIT_FAST);
    mysql_mutex_init(NULL, &global_timeout_cache->timeout_lock, MY_MUTEX_INIT_FAST);

#if __CONSISTEND_READ__
    mysql_mutex_init(NULL, &global_consistend_cache.consistend_lock, MY_MUTEX_INIT_FAST);
    LIST_INIT(global_consistend_cache.consistend_read_lst);
#endif

    strcpy(proxy_version_ptr, MYSQL_SERVER_VERSION);
    global_proxy_config.config_version = 0;
    global_proxy_config.rules_version = 0;
    global_proxy_config.config_write_lock();
    err = proxy_load_config(true, argc, argv, groups);
    global_proxy_config.config_unlock();
    if (err)
        proxy_config_deinit();
    return err;
}

int proxy_config_reload()
{
    int error;

    if (global_proxy_config.setting)
    {
        my_error(ER_CONFIG_IS_SETTING, MYF(0));
        return true;
    }

    global_proxy_config.setting = true;
    proxy_config_deinit();
    error = proxy_load_config(false, &orig_argc, &orig_argv, NULL);
    global_proxy_config.setting = false;

    return error;
}

#if __CONSISTEND_READ__
int proxy_create_consistend_read_table()
{
    MYSQL* conn;
    conn = proxy_get_config_server_connection(false);
    str_t str_sql;
    str_t str_createdb;

    if (conn == NULL)
        return true;
        
    /* if the config center is existed, then store it there, otherwise
     * store to the mysql db of proxy backend server*/
    str_init(&str_sql);
    str_init(&str_createdb);
    str_append(&str_sql, "CREATE TABLE ");
    /*TODO need to check the validation of proxy_namespace, 
     * only include number and character */
    str_append(&str_sql, "arkproxy_consistend_read");
    str_append(&str_sql, ".update_timestamp");
    str_append(&str_createdb, "CREATE DATABASE ");
    str_append(&str_createdb, "arkproxy_consistend_read");

    if (str_get_len(&str_createdb) > 0)
    {
        if (mysql_real_query(conn, str_get(&str_createdb), str_get_len(&str_createdb)))
        {
            if (mysql_errno(conn) != 1007/*ER_DB_CREATE_EXISTS*/)
            {
                my_error(ER_START_UP, MYF(0), "proxy_user", mysql_error(conn));
                str_deinit(&str_sql);
                str_deinit(&str_createdb);
                mysql_close(conn);
                return true;
            }
        }
    }

    str_append(&str_sql, "(instance int comment 'mysql cluster id', ");
    str_append(&str_sql, "dbname varchar(32) not null comment 'db name', ");
    str_append(&str_sql, "tablename varchar(32) not null comment 'table name', ");
    str_append(&str_sql, "updatetime bigint not null comment 'update time', ");
    str_append(&str_sql, "primary key (instance, dbname, tablename))");
    str_append(&str_sql, "engine innodb comment 'arkproxy_consistend_read table'");

    if (mysql_real_query(conn, str_get(&str_sql), str_get_len(&str_sql)))
    {
        if (mysql_errno(conn) != 1050/*ER_TABLE_EXISTS_ERROR*/)
        {
            my_error(ER_START_UP, MYF(0), "arkproxy_consistend_read.update_timestamp", 
                mysql_error(conn));
            str_deinit(&str_sql);
            str_deinit(&str_createdb);
            mysql_close(conn);
            return true;
        }
    }

    str_deinit(&str_sql);
    str_deinit(&str_createdb);
    mysql_close(conn);
    return false;
}
    
#endif
uchar *proxy_user_get_key(proxy_user_t* proxy_user, size_t *length,
                           my_bool not_used __attribute__((unused)))
{
    if (proxy_user->hash_key[0] == '\0')
        sprintf(proxy_user->hash_key, "%s%s", proxy_user->user, proxy_user->host);

    *length= strlen(proxy_user->hash_key);
    return (uchar*)proxy_user->hash_key;
}

int proxy_load_config(int init, int *argc, char ***argv, char** groups)
{
    char* tmp_group[2];
    int ori_argc;
    char **ori_argv;
    THD* thd;
    char* new_config_groups[MAX_CONFIG_GROUP];
    acl_host_and_ip host_and_ip;

    LIST_INIT(global_proxy_config.server_lst);
    LIST_INIT(global_proxy_config.rw_server_lst);
    LIST_INIT(global_proxy_config.ro_server_lst);
    LIST_INIT(global_proxy_config.listen_lst);
    LIST_INIT(global_proxy_config.router_lst);
    LIST_INIT(global_proxy_config.white_ip_lst);
    LIST_INIT(global_proxy_config.incoming_lst);

    my_hash_init(&global_proxy_config.proxy_user, &my_charset_bin,
              4096, 0, 0, (my_hash_get_key)proxy_user_get_key, NULL, 0);

    tmp_group[1] = NULL;
    ori_argv = *argv;
    ori_argc = *argc;
    if (groups == NULL)
    {
        /* here, load all the group in cnf file, except the 
         * group name in array load_default_groups, pick the 
         * groups defined by userselves */
        new_config_groups[0] = NULL;
        if (load_defaults(MYSQL_CONFIG_NAME, load_default_groups, 
              &ori_argc, &ori_argv, new_config_groups))
            return true;
        groups = new_config_groups;
    }

    ori_argv = *argv;
    ori_argc = *argc;
    for (; *groups; groups++)
    {
        tmp_group[0] = *groups;
        if (load_defaults(MYSQL_CONFIG_NAME, (const char **)&tmp_group, &ori_argc, &ori_argv, NULL))
            return true;

        proxy_reset_config();
        my_getopt_skip_unknown = 0;
        if (handle_options(&ori_argc, &ori_argv, proxy_config_options, NULL))
            return true;
        /* check the validate of this group, copy the value to struct proxy_config_t*/
        if (proxy_set_config(tmp_group[0]))
            return true;

        /* next group to do */
        ori_argv = *argv;
        ori_argc = *argc;
    }
    
    /* at last, to check the config validations */
    if (proxy_check_config())
        return true;

    if (init && proxy_config_thd_init())
    {
        sql_print_warning("create backend server connection failed: %s", 
            current_thd->get_stmt_da()->message());
        return true;
    }

#if __CONSISTEND_READ__
    if (proxy_create_consistend_read_table())
    {
        sql_print_warning("create proxy consistend read table error: %s", 
            current_thd->get_stmt_da()->message());
        proxy_config_deinit();
        return true;
    }
#endif

    if (proxy_create_user_config())
    {
        sql_print_warning("create proxy user config table error: %s", 
            current_thd->get_stmt_da()->message());
        proxy_config_deinit();
        return true;
    }

    if (proxy_load_users())
    {
        sql_print_warning("proxy user config loading error: %s", 
            current_thd->get_stmt_da()->message());
        proxy_config_deinit();
        return true;
    }

    if (proxy_create_rules_config())
    {
        sql_print_warning("create proxy rules config table error: %s", 
            current_thd->get_stmt_da()->message());
        proxy_config_deinit();
        return true;
    }

    if (proxy_load_rules())
    {
        sql_print_warning("proxy rules config loading error: %s", 
            current_thd->get_stmt_da()->message());
        proxy_config_deinit();
        return true;
    }

    if (get_local_IP(proxy_local_ip))
    {
        sql_print_error("Could not get local ip, please check backend "
            "mysql server is running or not.");
        proxy_config_deinit();
        return true;
    }

    update_hostname(&host_and_ip, proxy_server_address);
    if (!compare_hostname(&host_and_ip, NULL, proxy_local_ip))
    {
        sql_print_error("proxy_server_address(%s) is invalid, "
            "it is incompatible with proxy real ip(%s)", 
            proxy_server_address, proxy_local_ip);
        proxy_config_deinit();
        return true;
    }

    uchar guid[MY_UUID_SIZE];
    my_uuid(guid);
    memset(proxy_server_uuid, 0, sizeof(128));
    my_uuid2str(guid, (char *)proxy_server_uuid);

    if (update_incoming_addresses_low())
    {
        sql_print_error("Too many cluster node, max 32 allowed");
        proxy_config_deinit();
        return true;
    }

    return false;
}

char* proxy_get_shell_username()
{
    proxy_listens_t* listen;

    listen = LIST_GET_FIRST(global_proxy_config.listen_lst);
    while (listen)
    {
        if (listen->listen_type == LISTEN_TYPE_SHELL)
        {
            return listen->shell_username;
        }

        listen  = LIST_GET_NEXT(link, listen);
    }

    return NULL;
}

char* proxy_get_shell_password()
{
    proxy_listens_t* listen;

    listen = LIST_GET_FIRST(global_proxy_config.listen_lst);
    while (listen)
    {
        if (listen->listen_type == LISTEN_TYPE_SHELL)
        {
            return listen->shell_password;
        }

        listen  = LIST_GET_NEXT(link, listen);
    }

    return NULL;
}

MYSQL* proxy_get_config_server_connection(int need_lock)
{
    proxy_servers_t* server_node;
    backend_conn_t* conn;

    conn = (backend_conn_t*)my_malloc(sizeof(backend_conn_t), MY_ZEROFILL);
    if (metadata_not_local())
    {
        init_backend_conn_info(conn, proxy_config_host, proxy_config_user,
                               proxy_config_passwd, proxy_config_port);
    }
    else 
    {
        if (need_lock)
            global_proxy_config.config_read_lock();

        /* find one avialable node to get connection 
         * if the there is no avialable node, then can not get connection */
        proxy_server_t* server = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
        while (server)
        {
            if (server->server->server_status == SERVER_STATUS_ONLINE)
            {
                server_node = server->server;
                if (metadata_not_local())
                {
                    init_backend_conn_info(conn, proxy_config_host, proxy_config_user,
                                           proxy_config_passwd, proxy_config_port);
                }
                else
                {
                    init_backend_conn_info(conn, server_node->backend_host, backend_user,
                                           backend_passwd, server_node->backend_port);
                }
                break;
            }

            server = LIST_GET_NEXT(link, server);
        }

        if (!server)
            conn->inited = false;

        if (need_lock)
            global_proxy_config.config_unlock();
    }

    return get_backend_connection(NULL, conn);
}

MYSQL* proxy_get_trace_server_connection_with_con(backend_conn_t* conn)
{
    proxy_servers_t* server_node;
    if (!conn->conn_inited())
    {
        if (trace_metadata_not_local())
        {
            init_backend_conn_info(conn, proxy_trace_storage_host, proxy_trace_storage_user,
                                   proxy_trace_storage_passwd, proxy_trace_storage_port);
        }
        else
        {
            global_proxy_config.config_read_lock();
            proxy_server_t* server = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
            if (server)
            {
                server_node = server->server;
                if (trace_metadata_not_local())
                    init_backend_conn_info(conn, proxy_trace_storage_host, proxy_trace_storage_user,
                                           proxy_trace_storage_passwd, proxy_trace_storage_port);
                else
                    init_backend_conn_info(conn, server_node->backend_host, backend_user,
                                           backend_passwd, server_node->backend_port);
            }
            else
            {
                conn->inited = false;
            }
            global_proxy_config.config_unlock();
        }
    }
    return get_backend_connection(NULL, conn);
}

MYSQL* proxy_get_config_server_connection_with_con(backend_conn_t* conn)
{
    proxy_servers_t* server_node;
    if (!conn->conn_inited())
    {
        if (metadata_not_local())
        {
            init_backend_conn_info(conn, proxy_config_host, proxy_config_user,
                                   proxy_config_passwd, proxy_config_port);
        }
        else
        {
            global_proxy_config.config_read_lock();
            proxy_server_t* server = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
            if (server)
            {
                server_node = server->server;
                if (metadata_not_local())
                    init_backend_conn_info(conn, proxy_config_host, proxy_config_user,
                                           proxy_config_passwd, proxy_config_port);
                else
                    init_backend_conn_info(conn, server_node->backend_host, backend_user,
                                           backend_passwd, server_node->backend_port);
            }
            else
            {
                conn->inited = false;
            }
            global_proxy_config.config_unlock();
        }
    }
    return get_backend_connection(NULL, conn);
}

int proxy_get_config_database(char* dbname)
{
    if (trace_metadata_not_local())
    {
        strcpy(dbname, proxy_namespace);
        return false;
    }
    else
    {
        strcpy(dbname, "mysql");
        return true;
    }
}

int proxy_create_rules_config()
{
    MYSQL* conn;
    conn = proxy_get_config_server_connection(false);
    str_t str_sql;
    str_t str_createdb;

    if (conn == NULL)
        return true;
        
    /* if the config center is existed, then store it there, otherwise
     * store to the mysql db of proxy backend server*/
    str_init(&str_sql);
    str_init(&str_createdb);
    if (metadata_not_local())
    {
        str_append(&str_sql, "CREATE TABLE ");
        /*TODO need to check the validation of proxy_namespace, 
         * only include number and character */
        str_append(&str_sql, proxy_namespace);
        str_append(&str_sql, ".proxy_rules");
        str_append(&str_createdb, "CREATE DATABASE ");
        str_append(&str_createdb, proxy_namespace);
    }
    else
    {
        str_append(&str_sql, "CREATE TABLE mysql.proxy_rules");
    }

    if (str_get_len(&str_createdb) > 0)
    {
        if (mysql_real_query(conn, str_get(&str_createdb), str_get_len(&str_createdb)))
        {
            if (mysql_errno(conn) != 1007/*ER_DB_CREATE_EXISTS*/)
            {
                my_error(ER_START_UP, MYF(0), "proxy_rules", mysql_error(conn));
                str_deinit(&str_sql);
                str_deinit(&str_createdb);
                mysql_close(conn);
                return true;
            }
        }
    }

    str_append(&str_sql, "(rule_id INT PRIMARY KEY AUTO_INCREMENT NOT NULL, ");
    str_append(&str_sql, "active INT NOT NULL DEFAULT 0, ");
    str_append(&str_sql, "username VARCHAR(256), ");
    str_append(&str_sql, "schemaname VARCHAR(256), ");
    //str_append(&str_sql, "flagIN INT NOT NULL DEFAULT 0, ");
    str_append(&str_sql, "client_addr VARCHAR(256), ");
    str_append(&str_sql, "proxy_addr VARCHAR(256), ");
    str_append(&str_sql, "proxy_port INT, ");
    str_append(&str_sql, "digest VARCHAR(256), ");
    str_append(&str_sql, "match_digest text, ");
    str_append(&str_sql, "match_pattern text, ");
    str_append(&str_sql, "negate_match_pattern INT NOT NULL DEFAULT 0, ");
    str_append(&str_sql, "re_modifiers VARCHAR(256) DEFAULT 'CASELESS', ");
    //str_append(&str_sql, "flagOUT INT, ");
    str_append(&str_sql, "replace_pattern text, ");
    str_append(&str_sql, "destination VARCHAR(256), ");
    //str_append(&str_sql, "reconnect INT DEFAULT NULL, ");
    str_append(&str_sql, "timeout INT UNSIGNED, ");
    //str_append(&str_sql, "retries INT, ");
    //str_append(&str_sql, "delay INT UNSIGNED, ");
    //str_append(&str_sql, "mirror_flagOUT INT UNSIGNED, ");
    //str_append(&str_sql, "mirror_hostgroup INT UNSIGNED, ");
    str_append(&str_sql, "error_msg VARCHAR(1024), ");
    //str_append(&str_sql, "sticky_conn INT, ");
    //str_append(&str_sql, "multiplex INT, ");
    str_append(&str_sql, "log INT, ");
    //str_append(&str_sql, "apply INT NOT NULL DEFAULT 0, ");
    str_append(&str_sql, "comment text) ");
    str_append(&str_sql, "engine innodb comment 'rule config table'");

    if (mysql_real_query(conn, str_get(&str_sql), str_get_len(&str_sql)))
    {
        if (mysql_errno(conn) != 1050/*ER_TABLE_EXISTS_ERROR*/)
        {
            my_error(ER_START_UP, MYF(0), "proxy_user", mysql_error(conn));
            str_deinit(&str_sql);
            str_deinit(&str_createdb);
            mysql_close(conn);
            return true;
        }
    }

    str_deinit(&str_sql);
    str_deinit(&str_createdb);
    mysql_close(conn);
    return false;
}

int proxy_create_user_config()
{
    MYSQL* conn;
    conn = proxy_get_config_server_connection(false);
    str_t str_sql;
    str_t str_createdb;

    if (conn == NULL)
        return true;
        
    /* if the config center is existed, then store it there, otherwise
     * store to the mysql db of proxy backend server*/
    str_init(&str_sql);
    str_init(&str_createdb);
    if (metadata_not_local())
    {
        str_append(&str_sql, "CREATE TABLE ");
        /*TODO need to check the validation of proxy_namespace, 
         * only include number and character */
        str_append(&str_sql, proxy_namespace);
        str_append(&str_sql, ".proxy_user");
        str_append(&str_createdb, "CREATE DATABASE ");
        str_append(&str_createdb, proxy_namespace);
    }
    else
    {
        str_append(&str_sql, "CREATE TABLE mysql.proxy_user");
    }

    if (str_get_len(&str_createdb) > 0)
    {
        if (mysql_real_query(conn, str_get(&str_createdb), str_get_len(&str_createdb)))
        {
            if (mysql_errno(conn) != 1007/*ER_DB_CREATE_EXISTS*/)
            {
                my_error(ER_START_UP, MYF(0), "proxy_user", mysql_error(conn));
                str_deinit(&str_sql);
                str_deinit(&str_createdb);
                mysql_close(conn);
                return true;
            }
        }
    }

    str_append(&str_sql, "(id int auto_increment primary key comment 'auto increment', ");
    str_append(&str_sql, "user_name varchar(32) not null comment 'user name', ");
    str_append(&str_sql, "host varchar(128) not null comment 'host address', ");
    str_append(&str_sql, "schema_name varchar(64) not null comment 'schema name for user', ");
    str_append(&str_sql, "max_connections int not null comment 'max connection for user', ");
    str_append(&str_sql, "unique key (user_name, host))");
    str_append(&str_sql, "engine innodb comment 'user config table'");

    if (mysql_real_query(conn, str_get(&str_sql), str_get_len(&str_sql)))
    {
        if (mysql_errno(conn) != 1050/*ER_TABLE_EXISTS_ERROR*/)
        {
            my_error(ER_START_UP, MYF(0), "proxy_user", mysql_error(conn));
            str_deinit(&str_sql);
            str_deinit(&str_createdb);
            mysql_close(conn);
            return true;
        }
    }

    str_deinit(&str_sql);
    str_deinit(&str_createdb);
    mysql_close(conn);
    return false;
}

static bool rules_sort_comp_function (QP_rule_t * a, QP_rule_t * b) 
{ 
    return (a->rule_id < b->rule_id); 
}

int proxy_load_rules()
{
    MYSQL* conn;
    MYSQL_RES *     source_res;
    MYSQL_ROW       source_row;
    str_t str_sql;
    QP_rule_t* proxy_rule;

    conn = proxy_get_config_server_connection(false);
    if (conn == NULL)
        return true;
    /* if the config center is existed, then store it there, otherwise
     * store to the mysql db of proxy backend server*/
    str_init(&str_sql);

    if (metadata_not_local())
    {
        str_append(&str_sql, "SELECT * FROM ");
        str_append(&str_sql, proxy_namespace);
        str_append(&str_sql, ".proxy_rules");
    }
    else
    {
        str_append(&str_sql, "SELECT * FROM mysql.proxy_rules");
    }

    if (mysql_real_query(conn, str_get(&str_sql), str_get_len(&str_sql)) ||
        (source_res = mysql_store_result(conn)) == NULL)
    {
        my_message(mysql_errno(conn), mysql_error(conn), MYF(0));
        mysql_close(conn);
        return true;
    }

    __reset_rules(&global_proxy_config.rules);
    source_row = mysql_fetch_row(source_res);
    while (source_row)
    {
        int i = 0;
        int rule_id = source_row[i] == NULL ? 0 : (int)strtoll(source_row[i], NULL, 10);
        int active =  source_row[++i] == NULL ? 0 : (bool)strtoll(source_row[i], NULL, 10);
        char* username = source_row[++i] == NULL ? NULL : strdup(source_row[i]);
        char* schemaname =source_row[++i] == NULL ? NULL : strdup(source_row[i]);
        char* client_addr= source_row[++i] == NULL ? NULL : strdup(source_row[i]);
        char* proxy_addr=  source_row[++i] == NULL ? NULL : strdup(source_row[i]);
        int proxy_port = source_row[++i] == NULL ? 0 : (int)strtoll(source_row[i], NULL, 10);
        char* digest = source_row[++i] == NULL ? NULL : strdup(source_row[i]);
        char* match_digest = source_row[++i] == NULL ? NULL : strdup(source_row[i]);
        char* match_pattern = source_row[++i] == NULL ? NULL : strdup(source_row[i]);
        int negate_match_pattern = 
          source_row[++i] == NULL ? 0 : (bool)strtoll(source_row[i], NULL, 10);
        char * re_modifiers = source_row[++i] == NULL ? NULL : strdup(source_row[i]);
        char* replace_pattern = source_row[++i] == NULL ? NULL : strdup(source_row[i]);
        char* destination= source_row[++i] == NULL ? NULL : strdup(source_row[i]);
        int timeout= source_row[++i] == NULL ? 0 : (int)strtoll(source_row[i], NULL, 10);
        char* error_msg = source_row[++i] == NULL ? NULL : strdup(source_row[i]);
        int log = source_row[++i] == NULL ? 0 : (int)strtoll(source_row[i], NULL, 10);
        char* comment = source_row[++i] == NULL ? NULL : strdup(source_row[i])/* comment */;
        proxy_rule = new_query_rule(
            rule_id ,
            active ,
            username ,
            schemaname ,
            0, 
            client_addr ,
            proxy_addr ,
            proxy_port ,
            digest ,
            match_digest ,
            match_pattern ,
            negate_match_pattern ,
            re_modifiers ,
            0, 
            replace_pattern ,
            destination ,
            0, /* cache++ittl */
            0, /* reconnect */
            timeout ,
            0,/* retries */
            0, /* delay */
            0, /* next_++iuery_flagIN */
            0, /* mirror_flagOUT */
            0, /* mirror_hostgroup */
            error_msg ,
            NULL,/* OK_++isg */
            0, /* sticky_conn */
            0, /* multiplex */
            log,
            0, /* apply */
            comment);
        if (!proxy_rule)
        {
            source_row = mysql_fetch_row(source_res);
            continue;
        }

        global_proxy_config.rules.push_back(proxy_rule);
        source_row = mysql_fetch_row(source_res);
    }

    std::sort (global_proxy_config.rules.begin(), 
        global_proxy_config.rules.end(), 
        rules_sort_comp_function);
    ++global_proxy_config.rules_version;
    mysql_free_result(source_res);
    mysql_close(conn);
    return false;
}

int proxy_load_users()
{
    MYSQL* conn;
    MYSQL_RES *     source_res;
    MYSQL_ROW       source_row;
    str_t str_sql;
    proxy_user_t* proxy_user;

    conn = proxy_get_config_server_connection(false);
    if (conn == NULL)
        return true;
    /* if the config center is existed, then store it there, otherwise
     * store to the mysql db of proxy backend server*/
    str_init(&str_sql);

    if (metadata_not_local())
    {
        str_append(&str_sql, "SELECT * FROM ");
        str_append(&str_sql, proxy_namespace);
        str_append(&str_sql, ".proxy_user");
    }
    else
    {
        str_append(&str_sql, "SELECT * FROM mysql.proxy_user");
    }

    if (mysql_real_query(conn, str_get(&str_sql), str_get_len(&str_sql)) ||
        (source_res = mysql_store_result(conn)) == NULL)
    {
        my_message(mysql_errno(conn), mysql_error(conn), MYF(0));
        mysql_close(conn);
        return true;
    }

    /* reset global_proxy_config.proxy_user */
    for (uint i=0; i < global_proxy_config.proxy_user.records; i++)
    {
        proxy_user = (proxy_user_t*)my_hash_element(&global_proxy_config.proxy_user, i);
        my_free(proxy_user);
    }

    source_row = mysql_fetch_row(source_res);
    while (source_row)
    {
        proxy_user = (proxy_user_t*)my_malloc(sizeof(proxy_user_t), MY_ZEROFILL);
        strcpy(proxy_user->user, source_row[1]);
        strcpy(proxy_user->host, source_row[2]);
        strcpy(proxy_user->dbname, source_row[3]);
        proxy_user->max_connections = strtoll(source_row[4], NULL, 10);
        if (my_hash_insert(&global_proxy_config.proxy_user, (uchar*) proxy_user))
        {
            mysql_free_result(source_res);
            mysql_close(conn);
            return true;
        }

        source_row = mysql_fetch_row(source_res);
    }

    mysql_free_result(source_res);
    mysql_close(conn);
    return false;
}

int
proxy_decr_user_connections(THD* thd)
{
    Security_context *sctx= thd->security_ctx;
    char key[256];
    int key_length;
    my_hash_value_type hash_value;
    proxy_user_t* proxy_user;

    sprintf(key, "%s%s", (char*)sctx->external_user, sctx->ip);
    key_length = strlen(key);
    hash_value= my_calc_hash(&global_proxy_config.proxy_user, (uchar*) key, key_length);

    proxy_user = (proxy_user_t*)my_hash_search_using_hash_value(&global_proxy_config.proxy_user,
                hash_value, (uchar*) key, key_length);

    if (proxy_user && thd->conn_count_added)
    {
        // global_proxy_config.config_write_lock();
        my_atomic_add64(&proxy_user->conn_count, -1);
        // global_proxy_config.config_unlock();
    }

    return false;
}
