#include "sql_class.h"
#include "sql_connect.h"
#include "sql_parse.h"
#include "sql_show.h"
#include <mysql.h>
#include "set_var.h"
#include "sql_cte.h"
#include "sql_base.h"
#include "sp.h"
#include "replication.h"
#include "sql_trace.h"
#include "sql_rules.h"

int proxy_load_rules();
extern int trace_digest_inited ;
extern int trace_sql_inited ;
extern void mysqld_list_thread_pool(THD *thd,const char *user, bool verbose);
extern MYSQL* get_backend_connection(THD* thd, backend_conn_t* conn);
int metadata_not_local();
extern trace_cache_t* global_trace_cache;

extern bool handle_select(THD *thd, LEX *lex, select_result *result,
                          ulong setup_tables_done_option);

void localtime_to_TIME(MYSQL_TIME *to, struct tm *from);
int mysql_local_show_variables(THD *thd, int showall);
MYSQL* proxy_get_config_server_connection(int need_lock);
int lic_gen_lic(char*	key_file_name, void*	lic_info);
void proxy_help(THD* thd);
bool write_status_array_for_proxy( THD *thd, SHOW_VAR *variables, str_t* outstr);
bool write_out_var_exceptions(const char* name,str_t* outstr);
#define HELP_SIZE 14
#define HELP_CMD_LENGTH 512

typedef struct help_struct help_t;
struct help_struct
{
    char*               command;
    char*               comment;
};

int str_mysql_dup_char(
    char* src,
    str_t* dest,
    char chr
)
{
    int ret = 0;
    char* p = src;
    while (*src)
    {
        if (*src == '\\')
            ret=1;
        //if (*src == chr && (p == src || *(src-1) != '\\'))
        if (*src == chr)
        {
            str_append_1(dest, &chr);
            str_append_1(dest, &chr);
        }
        else
        {
            str_append_1(dest, src);
        }

        src++;
    }

    return ret;
}

static help_t HELP_INFO[] =
{
    {(char*)"/*!999999 route to {write|read}*/ ...", (char*)"中间层的hint使用方式，"
      "在每条语句的最前面加上这些信息，就可以主动让ArkProxy"
        "将查询路由到写节点或者读节点，适用于对于主从延迟敏感的业务"},
    {(char*)"config reload", (char*)"将外部配置文件中的内容reload到内存。"
      "当手动修改配置文件中之后，可以通过config reload让其生效。"},
    {(char*)"config set server {servername} online/offline", (char*)"将指定server上下线"},
    {(char*)"config {add|delete|online|offline} route {read|write} server {servername}", (char*)"新增某个server的读写角色"},
    {(char*)"show backend servers", (char*)"查看后端配置数据库的状态"},
    {(char*)"show backend connections", (char*)"查看ArkProxy链接后端的信息"},
    {(char*)"show user config list", (char*)"可以查看配置user的信息"},
    {(char*)"show config cache", (char*)"查看当前缓存中的配置信息"},
    {(char*)"show trace status", (char*)"查看当前中间层关于trace状态的信息"},
    {(char*)"show trace full status", (char*)"查看当前中间层关于trace状态的更详细的信息"},
    {(char*)"show backend routes", (char*)"查看后台server对应route的状态"},
    {(char*)"show rule status", (char*)"查看当前全局Rule状态"},
    {(char*)"show variables [like \%name\%]", (char*)"显示管理用户的所有或指定variables"},
    {(char*)"config help", (char*)"显示所有的管理端口的命令帮助"},
    {(char*)"config add {read|write} server {servername} "
      "[server_attribute [,server_attribute] ...]; "
        "server_attribute: {host='string'|port=integer|max_lag=integer|weight=integer}", 
      (char*)"增加一个server节点，在server_attribute中指定了这个节点的属性。新增节点状态都是OFFLINE，如果需要上线，则将其手动上线"},
    {(char*)"config delete server {servername}", (char*)"根据servername来删除一个server节点"},
    {(char*)"config write outfile '{filename}'", (char*)"将当前配置，"
      "写出到外部文件中，文件名通过filename来指定。"},
    {(char*)"config set user '{username}'@'{userip}' max_user_connections={integer}", 
      (char*)"在有权限的基础上，可以再限制某用户可连接的最大连接数"},
    {(char*)"config set user '{username}'@'{userip}' databases={dbname}", 
      (char*)"在有权限的基础上，可以再限制可连接的后端数据库，不过这个功能暂未支持"},
    {(char*)"config delete {integer}", (char*)"用来删除某个设置项的。"
      "integer表示序号，序号需要从show config cache中拿到。"},
    {(char*)"config {delete all|reset}", (char*)"表示当前未生效的配置缓存都不要了"},
    {(char*)"config reload", (char*)"将外部配置文件中的内容reload到内存。"
      "当手动修改配置文件中之后，可以通过config reload让其生效。"},
    {(char*)"config flush", (char*)"设置完后，此时所有的配置还未生效，"
      "只是缓存到当前会话中的，需要通过config flush命令让其生效。"},
    {(char*)"config trace sql cache create", (char*)"用来创建sql cache，同时会创建digest缓存"},
    {(char*)"config trace cache create", (char*)"用来创建digest cache，此时不会去创建sql cache"},
    {(char*)"config trace sql cache close", (char*)"用来销毁sql cache，不会销毁digest cache"},
    {(char*)"config trace cache close", (char*)"用来销毁digest cache，同时会销毁sql cache"},
    {(char*)"config {add|delete|update} rule {[rule_id|active|username|schemaname|client_addr|proxy_addr|proxy_port|digest|match_digest|match_pattern|negate_match_pattern|re_modifiers|replace_pattern|destination|timeout|error_msg|log|comment] = value}...", (char*)"用来增加、修改、删除rule"},
    {NULL, NULL},
};

int proxy_config_update_rule(THD* thd)
{
    config_element_t* config_ele = NULL;
    config_element_t* ele = NULL;
    char tmp[1024];

    if (thd->config_cache == NULL)
    {
        thd->config_cache = (config_cache_t*)my_malloc(sizeof(config_cache_t), MY_ZEROFILL);
        LIST_INIT(thd->config_cache->config_lst);
    }

    config_ele = thd->config_ele;
    if (config_ele && config_ele->rule && (config_ele->rule->rule_id == 0 || 
          config_ele->rule->op_mask == RULE_TYPE_rule_id))
    {
        sprintf(tmp, "rule update config need and not only need rule_id");
        my_error(ER_CONFIG_ERROR, MYF(0), tmp);
        return true;
    }

    //ele = LIST_GET_FIRST(thd->config_cache->config_lst);
    //while (ele)
    //{
    //    if (ele->sub_command == CFG_RULE_DELETE)
    //    {
    //        if (ele->rule->rule_id == config_ele->rule->rule_id)
    //        {
    //            sprintf(tmp, "Delete rule with id '%d' have existed", ele->rule->rule_id);
    //            my_error(ER_CONFIG_ERROR, MYF(0), tmp);
    //            return true;
    //        }
    //    }

    //    ele = LIST_GET_NEXT(link, ele);
    //}

    config_ele->sequence = ++thd->config_sequence;
    config_ele->sub_command = thd->lex->sub_command;
    str_init(&config_ele->config_command);
    str_append_with_length(&config_ele->config_command, thd->query(), thd->query_length());
    LIST_ADD_LAST(link, thd->config_cache->config_lst, config_ele);
    thd->config_ele = NULL;
    my_ok(thd);

    return false;
}

int proxy_config_delete_rule(THD* thd)
{
    config_element_t* config_ele = NULL;
    config_element_t* ele = NULL;
    char tmp[1024];

    if (thd->config_cache == NULL)
    {
        thd->config_cache = (config_cache_t*)my_malloc(sizeof(config_cache_t), MY_ZEROFILL);
        LIST_INIT(thd->config_cache->config_lst);
    }

    config_ele = thd->config_ele;
    if (config_ele && config_ele->rule && (config_ele->rule->rule_id == 0 || 
          config_ele->rule->op_mask != RULE_TYPE_rule_id))
    {
        sprintf(tmp, "rule delete config need and only need rule_id");
        my_error(ER_CONFIG_ERROR, MYF(0), tmp);
        return true;
    }

    ele = LIST_GET_FIRST(thd->config_cache->config_lst);
    while (ele)
    {
        if (ele->sub_command == CFG_RULE_DELETE)
        {
            if (ele->rule->rule_id == config_ele->rule->rule_id)
            {
                sprintf(tmp, "Delete rule with id '%d' have existed", ele->rule->rule_id);
                my_error(ER_CONFIG_ERROR, MYF(0), tmp);
                return true;
            }
        }

        ele = LIST_GET_NEXT(link, ele);
    }

    config_ele->sequence = ++thd->config_sequence;
    config_ele->sub_command = thd->lex->sub_command;
    str_init(&config_ele->config_command);
    str_append_with_length(&config_ele->config_command, thd->query(), thd->query_length());
    LIST_ADD_LAST(link, thd->config_cache->config_lst, config_ele);
    thd->config_ele = NULL;
    my_ok(thd);

    return false;
}

int proxy_config_add_rule(THD* thd)
{
    config_element_t* config_ele = NULL;
    config_element_t* ele = NULL;
    char tmp[1024];

    if (thd->config_cache == NULL)
    {
        thd->config_cache = (config_cache_t*)my_malloc(sizeof(config_cache_t), MY_ZEROFILL);
        LIST_INIT(thd->config_cache->config_lst);
    }

    config_ele = thd->config_ele;
    if (config_ele->rule->rule_id == 0)
    {
        sprintf(tmp, "rule config need rule_id");
        my_error(ER_CONFIG_ERROR, MYF(0), tmp);
        return true;
    }

    ele = LIST_GET_FIRST(thd->config_cache->config_lst);
    while (ele)
    {
        if (ele->sub_command == CFG_RULE_ADD)
        {
            if (ele->rule->rule_id == config_ele->rule->rule_id)
            {
                sprintf(tmp, "Rule with id '%d' have existed", ele->rule->rule_id);
                my_error(ER_CONFIG_ERROR, MYF(0), tmp);
                return true;
            }
        }

        ele = LIST_GET_NEXT(link, ele);
    }

    config_ele->sequence = ++thd->config_sequence;
    config_ele->sub_command = thd->lex->sub_command;
    str_init(&config_ele->config_command);
    str_append_with_length(&config_ele->config_command, thd->query(), thd->query_length());
    LIST_ADD_LAST(link, thd->config_cache->config_lst, config_ele);
    thd->config_ele = NULL;
    my_ok(thd);

    return false;
}

int proxy_config_onoffline_route(THD* thd)
{
    config_element_t* config_ele = NULL;
    config_element_t* ele = NULL;
    char tmp[1024];
    proxy_server_t* server;
    proxy_servers_t* server_real;

    if (thd->config_cache == NULL)
    {
        thd->config_cache = (config_cache_t*)my_malloc(sizeof(config_cache_t), MY_ZEROFILL);
        LIST_INIT(thd->config_cache->config_lst);
    }

    config_ele = thd->config_ele;

    ele = LIST_GET_FIRST(thd->config_cache->config_lst);
    while (ele)
    {
        if (ele->sub_command == CFG_ONOFFLINE_ROUTE)
        {
            if (!strcasecmp(config_ele->server_name, ele->server_name) &&
                config_ele->type == ele->type)
            {
                sprintf(tmp, "Server '%s' route '%s' setting have existed", 
                    ele->server_name, ele->type == ROUTER_TYPE_RW ? "write" : "read");
                my_error(ER_CONFIG_ERROR, MYF(0), tmp);
                return true;
            }
        }

        ele = LIST_GET_NEXT(link, ele);
    }

    if (config_ele->type == ROUTER_TYPE_RW)
    {
        server = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
        while (server)
        {
            if ((ulong)server->route->router_type == config_ele->type &&
                !strcasecmp(server->server->server_name, config_ele->server_name))
                break;

            server = LIST_GET_NEXT(link, server);
        }
        if (server == NULL)
        {
            sprintf(tmp, "Server '%s' route 'read' not existed", config_ele->server_name);
            my_error(ER_CONFIG_ERROR, MYF(0), tmp);
            return true;
        }
    }

    if (config_ele->type == ROUTER_TYPE_RO)
    {
        server = LIST_GET_FIRST(global_proxy_config.ro_server_lst);
        while (server)
        {
            if ((ulong)server->route->router_type == config_ele->type &&
                !strcasecmp(server->server->server_name, config_ele->server_name))
                break;

            server = LIST_GET_NEXT(link, server);
        }

        if (server == NULL)
        {
            sprintf(tmp, "Server '%s' route 'read' not existed", config_ele->server_name);
            my_error(ER_CONFIG_ERROR, MYF(0), tmp);
            return true;
        }
    }

    server_real = LIST_GET_FIRST(global_proxy_config.server_lst);
    while (server_real)
    {
        if (!strcasecmp(server_real->server_name, config_ele->server_name))
        {
            break;
        }

        server_real = LIST_GET_NEXT(link, server_real);
    }

    if (server_real == NULL)
    {
        sprintf(tmp, "Server '%s' have not existed", config_ele->server_name);
        my_error(ER_CONFIG_ERROR, MYF(0), tmp);
        return true;
    }

    config_ele->sequence = ++thd->config_sequence;
    config_ele->sub_command = thd->lex->sub_command;
    str_init(&config_ele->config_command);
    str_append_with_length(&config_ele->config_command, thd->query(), thd->query_length());
    LIST_ADD_LAST(link, thd->config_cache->config_lst, config_ele);
    thd->config_ele = NULL;
    my_ok(thd);
    return true;
}

int proxy_config_delete_route(THD* thd)
{
    config_element_t* config_ele = NULL;
    config_element_t* ele = NULL;
    char tmp[1024];
    proxy_server_t* server;
    proxy_servers_t* server_real;
    int can_delete = false;

    if (thd->config_cache == NULL)
    {
        thd->config_cache = (config_cache_t*)my_malloc(sizeof(config_cache_t), MY_ZEROFILL);
        LIST_INIT(thd->config_cache->config_lst);
    }

    config_ele = thd->config_ele;

    ele = LIST_GET_FIRST(thd->config_cache->config_lst);
    while (ele)
    {
        if (ele->sub_command == CFG_DELETE_ROUTE)
        {
            if (!strcasecmp(config_ele->server_name, ele->server_name) &&
                config_ele->type == ele->type)
            {
                sprintf(tmp, "Server '%s' route '%s' have deleted", 
                    ele->server_name, ele->type == ROUTER_TYPE_RW ? "write" : "read");
                my_error(ER_CONFIG_ERROR, MYF(0), tmp);
                return true;
            }
        }

        ele = LIST_GET_NEXT(link, ele);
    }

    if (config_ele->type == ROUTER_TYPE_RW)
    {
        server = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
        while (server)
        {
            if ((ulong)server->route->router_type == config_ele->type &&
                !strcasecmp(server->server->server_name, config_ele->server_name))
                break;

            server = LIST_GET_NEXT(link, server);
        }
    }
    else 
    {
        server = LIST_GET_FIRST(global_proxy_config.ro_server_lst);
        while (server)
        {
            if ((ulong)server->route->router_type == config_ele->type &&
                !strcasecmp(server->server->server_name, config_ele->server_name))
                break;

            server = LIST_GET_NEXT(link, server);
        }
    }

    if (server == NULL)
    {
        sprintf(tmp, "Server '%s' route '%s' not exists", 
            config_ele->server_name, config_ele->type == ROUTER_TYPE_RW ? "write" : "read");
        my_error(ER_CONFIG_ERROR, MYF(0), tmp);
        return true;
    }

    server_real = LIST_GET_FIRST(global_proxy_config.server_lst);
    while (server_real)
    {
        if (!strcasecmp(server_real->server_name, config_ele->server_name))
        {
            if ((config_ele->type == ROUTER_TYPE_RO && server_real->noread_routed) ||
                (config_ele->type == ROUTER_TYPE_RW && server_real->nowrite_routed))
                can_delete = true;
            break;
        }

        server_real = LIST_GET_NEXT(link, server_real);
    }

    if (server_real == NULL || can_delete == false)
    {
        sprintf(tmp, "Server '%s' have not existed, or server route is online", config_ele->server_name);
        my_error(ER_CONFIG_ERROR, MYF(0), tmp);
        return true;
    }

    config_ele->sequence = ++thd->config_sequence;
    config_ele->sub_command = thd->lex->sub_command;
    str_init(&config_ele->config_command);
    str_append_with_length(&config_ele->config_command, thd->query(), thd->query_length());
    LIST_ADD_LAST(link, thd->config_cache->config_lst, config_ele);
    thd->config_ele = NULL;
    my_ok(thd);
    return true;

}

int proxy_config_add_route(THD* thd)
{
    config_element_t* config_ele = NULL;
    config_element_t* ele = NULL;
    char tmp[1024];
    proxy_server_t* server;
    proxy_servers_t* server_real;

    if (thd->config_cache == NULL)
    {
        thd->config_cache = (config_cache_t*)my_malloc(sizeof(config_cache_t), MY_ZEROFILL);
        LIST_INIT(thd->config_cache->config_lst);
    }

    config_ele = thd->config_ele;

    ele = LIST_GET_FIRST(thd->config_cache->config_lst);
    while (ele)
    {
        if (ele->sub_command == CFG_ADD_ROUTE)
        {
            if (!strcasecmp(config_ele->server_name, ele->server_name) &&
                config_ele->type == ele->type)
            {
                sprintf(tmp, "Server '%s' route '%s' have existed", 
                    ele->server_name, ele->type == ROUTER_TYPE_RW ? "write" : "read");
                my_error(ER_CONFIG_ERROR, MYF(0), tmp);
                return true;
            }
        }

        ele = LIST_GET_NEXT(link, ele);
    }

    server = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
    while (server)
    {
        if ((ulong)server->route->router_type == config_ele->type &&
            !strcasecmp(server->server->server_name, config_ele->server_name))
        {
            sprintf(tmp, "Server '%s' route 'write' have existed", config_ele->server_name);
            my_error(ER_CONFIG_ERROR, MYF(0), tmp);
            return true;
        }

        server = LIST_GET_NEXT(link, server);
    }
    
    server = LIST_GET_FIRST(global_proxy_config.ro_server_lst);
    while (server)
    {
        if ((ulong)server->route->router_type == config_ele->type &&
            !strcasecmp(server->server->server_name, config_ele->server_name))
        {
            sprintf(tmp, "Server '%s' route 'read' have existed", config_ele->server_name);
            my_error(ER_CONFIG_ERROR, MYF(0), tmp);
            return true;
        }

        server = LIST_GET_NEXT(link, server);
    }

    server_real = LIST_GET_FIRST(global_proxy_config.server_lst);
    while (server_real)
    {
        if (!strcasecmp(server_real->server_name, config_ele->server_name))
        {
            break;
        }

        server_real = LIST_GET_NEXT(link, server_real);
    }

    if (server_real == NULL)
    {
        sprintf(tmp, "Server '%s' have not existed", config_ele->server_name);
        my_error(ER_CONFIG_ERROR, MYF(0), tmp);
        return true;
    }

    config_ele->sequence = ++thd->config_sequence;
    config_ele->sub_command = thd->lex->sub_command;
    str_init(&config_ele->config_command);
    str_append_with_length(&config_ele->config_command, thd->query(), thd->query_length());
    LIST_ADD_LAST(link, thd->config_cache->config_lst, config_ele);
    thd->config_ele = NULL;
    my_ok(thd);
    return true;
}

int proxy_config_add_server(THD* thd)
{
    config_element_t* config_ele = NULL;
    config_element_t* ele = NULL;
    char tmp[1024];

    if (thd->config_cache == NULL)
    {
        thd->config_cache = (config_cache_t*)my_malloc(sizeof(config_cache_t), MY_ZEROFILL);
        LIST_INIT(thd->config_cache->config_lst);
    }

    config_ele = thd->config_ele;
    // if (config_ele->type == ROUTER_TYPE_RW && !proxy_multi_write_mode)
    // {
    //     sprintf(tmp, "Can not dynamic add write server node if proxy_multi_write_mode if OFF");
    //     my_error(ER_CONFIG_ERROR, MYF(0), tmp);
    //     return true;
    // }

    ele = LIST_GET_FIRST(thd->config_cache->config_lst);
    while (ele)
    {
        if (ele->sub_command == CFG_ADD_SERVER)
        {
            if (!strcmp(ele->hostname, config_ele->hostname) &&
                ele->port == config_ele->port &&
                ele->type == config_ele->type)
            {
                sprintf(tmp, "Server '%s' with host: '%s', port: %ld have existed", 
                    config_ele->server_name, config_ele->hostname, config_ele->port);
                my_error(ER_CONFIG_ERROR, MYF(0), tmp);
                return true;
            }

            if (!strcmp(ele->server_name, config_ele->server_name))
            {
                sprintf(tmp, "Server '%s' have existed", ele->server_name);
                my_error(ER_CONFIG_ERROR, MYF(0), tmp);
                return true;
            }
            
        }

        ele = LIST_GET_NEXT(link, ele);
    }

    if (config_ele->weight <= 0)
    {
        sprintf(tmp, "Server weight %ld is invalid", config_ele->weight);
        my_error(ER_CONFIG_ERROR, MYF(0), tmp);
        return true;
    }

    config_ele->sequence = ++thd->config_sequence;
    config_ele->sub_command = thd->lex->sub_command;
    str_init(&config_ele->config_command);
    str_append_with_length(&config_ele->config_command, thd->query(), thd->query_length());
    LIST_ADD_LAST(link, thd->config_cache->config_lst, config_ele);
    thd->config_ele = NULL;
    my_ok(thd);

    return false;
}

int proxy_config_delete_server(THD *thd)
{
    config_element_t* config_ele, *ele = NULL;
    char tmp[1024];
    if (thd->config_cache == NULL)
    {
        thd->config_cache = (config_cache_t*)my_malloc(sizeof(config_cache_t), MY_ZEROFILL);
        LIST_INIT(thd->config_cache->config_lst);
    }

    config_ele = thd->config_ele;
    ele = LIST_GET_FIRST(thd->config_cache->config_lst);
    while (ele)
    {
        if (ele->sub_command == CFG_DELETE_SERVER && !strcmp(ele->server_name, config_ele->server_name))
        {
            sprintf(tmp, "Server '%s' will be deleted already, you can execute config flush.",
                    config_ele->server_name);
            my_error(ER_CONFIG_ERROR, MYF(0), tmp);
            return true;
        }
        ele = LIST_GET_NEXT(link, ele);
    }

    config_ele->sequence = ++thd->config_sequence;
    config_ele->sub_command = thd->lex->sub_command;
    str_init(&config_ele->config_command);
    str_append_with_length(&config_ele->config_command, thd->query(), thd->query_length());
    LIST_ADD_LAST(link, thd->config_cache->config_lst, config_ele);
    thd->config_ele = NULL;

    my_ok(thd);
    return false;
}

int proxy_set_status_server(THD* thd)
{
    config_element_t* config_ele = NULL;
    config_element_t* new_ele = NULL;
    config_element_t* ele = NULL;

    if (thd->config_cache == NULL)
    {
        thd->config_cache = (config_cache_t*)my_malloc(sizeof(config_cache_t), MY_ZEROFILL);
        LIST_INIT(thd->config_cache->config_lst);
    }

    config_ele = thd->config_ele;

    /* check whether the server is setted already */
    ele = LIST_GET_FIRST(thd->config_cache->config_lst);
    while (ele)
    {
        if (ele->sub_command == CFG_CMD_USER_SET)
        {
            if (ele->sub_command == thd->lex->sub_command &&
                !strcmp(ele->hostname, config_ele->hostname) &&
                !strcmp(ele->username, config_ele->username))
            {
                /* if found, then update the node */
                new_ele = ele;
                break;
            }
        }
        else if (ele->sub_command == CFG_CMD_ONLINE_SERVER ||
            ele->sub_command == CFG_CMD_OFFLINE_SERVER)
        {
            if (!strcmp(ele->server_name, config_ele->server_name))
            {
                /* if found, then update the node */
                new_ele = ele;
                break;
            }
        }

        ele = LIST_GET_NEXT(link, ele);
    }

    if (!new_ele && config_ele)
    {
        config_ele->sequence = ++thd->config_sequence;
        config_ele->sub_command = thd->lex->sub_command;
        str_init(&config_ele->config_command);
        str_append_with_length(&config_ele->config_command, thd->query(), thd->query_length());
        LIST_ADD_LAST(link, thd->config_cache->config_lst, config_ele);
    }
    else if (config_ele)
    {
        if (ele->sub_command == CFG_CMD_USER_SET)
        {
            /* update the existed node */
            new_ele->max_connections = config_ele->max_connections;
            strcpy(new_ele->dbname, config_ele->dbname);
        }
        else if (ele->sub_command == CFG_CMD_ONLINE_SERVER || 
            ele->sub_command == CFG_CMD_OFFLINE_SERVER)
        {
            ele->sub_command = thd->lex->sub_command;
        }

        my_free(config_ele);
    }

    thd->config_ele = NULL;
    my_ok(thd);
    return false;
}

void
proxy_free_config_cache(THD* thd)
{
    config_element_t* ele;
    config_element_t* ele_next;

    if (thd->config_cache != NULL)
    {
        ele = LIST_GET_FIRST(thd->config_cache->config_lst);
        while (ele)
        {
            ele_next = LIST_GET_NEXT(link, ele);
            LIST_REMOVE(link, thd->config_cache->config_lst, ele);
            if (ele->rule)
                delete_query_rule(ele->rule);
            my_free(ele);

            ele = ele_next;
        }

        my_free(thd->config_cache);
        thd->config_cache = NULL;
    }

    if (thd->config_ele)
    {
        if (thd->config_ele->rule)
            delete_query_rule(thd->config_ele->rule);
        thd->config_ele->rule = NULL;
        my_free(thd->config_ele);
        thd->config_ele = NULL;
    }
}

int rule_get_update_set_values(str_t* str_sql, QP_rule_t *rule, int opmask)
{
    char field[1024];
    if (opmask & RULE_TYPE_rule_id)
    {
        str_append(str_sql, "rule_id=");
        sprintf(field, "%d", rule->rule_id);
        str_append(str_sql, field);
    }
    if (opmask & RULE_TYPE_active)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "active=");
        sprintf(field, "%d", rule->active);
        str_append(str_sql, field);
    }
    if (opmask & RULE_TYPE_username)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "username=");
        if (rule->username)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->username, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_schemaname)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "schemaname=");
        if (rule->schemaname)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->schemaname, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_client_addr)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "client_addr=");
        if (rule->client_addr)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->client_addr, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_proxy_addr)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "proxy_addr=");
        if (rule->proxy_addr)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->proxy_addr, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_proxy_port)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "proxy_port=");
        sprintf(field, "%d", rule->proxy_port);
        str_append(str_sql, field);
    }
    if (opmask & RULE_TYPE_digest)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "digest=");
        if (rule->digest_str)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->digest_str, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_match_digest)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "match_digest=");
        if (rule->match_digest)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->match_digest, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_match_pattern)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "match_pattern=");
        if (rule->match_pattern)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->match_pattern, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_negate_match_pattern)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "negate_match_pattern=");
        sprintf(field, "%d", rule->negate_match_pattern);
        str_append(str_sql, field);
    }
    if (opmask & RULE_TYPE_re_modifiers)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "re_modifiers=");
        if (rule->re_modifiers_str)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->re_modifiers_str, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_replace_pattern)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "replace_pattern=");
        if (rule->replace_pattern)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->replace_pattern, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_destination)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "destination=");
        if (rule->destination_hostgroup)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->destination_hostgroup, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_comment)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "comment=");
        if (rule->comment)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->comment, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_timeout)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "timeout=");
        sprintf(field, "%d", rule->timeout);
        str_append(str_sql, field);
    }
    if (opmask & RULE_TYPE_error_msg)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "error_msg=");
        if (rule->error_msg)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->error_msg, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_log)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "log=");
        sprintf(field, "%d", rule->log);
        str_append(str_sql, field);
    }
    if (opmask & RULE_TYPE_apply)
    {
        str_append(str_sql, ",");
        str_append(str_sql, "apply=");
        sprintf(field, "%d", rule->apply);
        str_append(str_sql, field);
    }

    return false;
}

int rule_get_insert_values(str_t* str_sql, QP_rule_t *rule, int opmask)
{
    char field[1024];
    if (opmask & RULE_TYPE_rule_id)
    {
        sprintf(field, "%d", rule->rule_id);
        str_append(str_sql, field);
    }
    if (opmask & RULE_TYPE_active)
    {
        str_append(str_sql, ",");
        sprintf(field, "%d", rule->active);
        str_append(str_sql, field);
    }
    if (opmask & RULE_TYPE_username)
    {
        str_append(str_sql, ",");
        if (rule->username)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->username, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_schemaname)
    {
        str_append(str_sql, ",");
        if (rule->schemaname)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->schemaname, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_client_addr)
    {
        str_append(str_sql, ",");
        if (rule->client_addr)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->client_addr, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_proxy_addr)
    {
        str_append(str_sql, ",");
        if (rule->proxy_addr)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->proxy_addr, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_proxy_port)
    {
        str_append(str_sql, ",");
        sprintf(field, "%d", rule->proxy_port);
        str_append(str_sql, field);
    }
    if (opmask & RULE_TYPE_digest)
    {
        str_append(str_sql, ",");
        if (rule->digest_str)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->digest_str, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_match_digest)
    {
        str_append(str_sql, ",");
        if (rule->match_digest)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->match_digest, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_match_pattern)
    {
        str_append(str_sql, ",");
        if (rule->match_pattern)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->match_pattern, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_negate_match_pattern)
    {
        str_append(str_sql, ",");
        sprintf(field, "%d", rule->negate_match_pattern);
        str_append(str_sql, field);
    }
    if (opmask & RULE_TYPE_re_modifiers)
    {
        str_append(str_sql, ",");
        if (rule->re_modifiers_str)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->re_modifiers_str, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_replace_pattern)
    {
        str_append(str_sql, ",");
        if (rule->replace_pattern)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->replace_pattern, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_destination)
    {
        str_append(str_sql, ",");
        if (rule->destination_hostgroup)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->destination_hostgroup, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_comment)
    {
        str_append(str_sql, ",");
        if (rule->comment)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->comment, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_timeout)
    {
        str_append(str_sql, ",");
        sprintf(field, "%d", rule->timeout);
        str_append(str_sql, field);
    }
    if (opmask & RULE_TYPE_error_msg)
    {
        str_append(str_sql, ",");
        if (rule->error_msg)
        {
            str_append(str_sql, "\'");
            str_mysql_dup_char(rule->error_msg, str_sql, '\'');
            str_append(str_sql, "\'");
        }
        else
            str_append(str_sql, "NULL");
    }
    if (opmask & RULE_TYPE_log)
    {
        str_append(str_sql, ",");
        sprintf(field, "%d", rule->log);
        str_append(str_sql, field);
    }
    if (opmask & RULE_TYPE_apply)
    {
        str_append(str_sql, ",");
        sprintf(field, "%d", rule->apply);
        str_append(str_sql, field);
    }

    return false;
}

int rule_get_insert_field(str_t* str_sql, int opmask)
{
    /* must have rule id */
    if (opmask & RULE_TYPE_rule_id)
        str_append(str_sql, "rule_id");
    if (opmask & RULE_TYPE_active)
        str_append(str_sql, ",active");
    if (opmask & RULE_TYPE_username)
        str_append(str_sql, ",username");
    if (opmask & RULE_TYPE_schemaname)
        str_append(str_sql, ",schemaname");
    if (opmask & RULE_TYPE_client_addr)
        str_append(str_sql, ",client_addr");
    if (opmask & RULE_TYPE_proxy_addr)
        str_append(str_sql, ",proxy_addr");
    if (opmask & RULE_TYPE_proxy_port)
        str_append(str_sql, ",proxy_port");
    if (opmask & RULE_TYPE_digest)
        str_append(str_sql, ",digest");
    if (opmask & RULE_TYPE_match_digest)
        str_append(str_sql, ",match_digest");
    if (opmask & RULE_TYPE_match_pattern)
        str_append(str_sql, ",match_pattern");
    if (opmask & RULE_TYPE_negate_match_pattern)
        str_append(str_sql, ",negate_match_pattern");
    if (opmask & RULE_TYPE_re_modifiers)
        str_append(str_sql, ",re_modifiers");
    if (opmask & RULE_TYPE_replace_pattern)
        str_append(str_sql, ",replace_pattern");
    if (opmask & RULE_TYPE_destination)
        str_append(str_sql, ",destination");
    if (opmask & RULE_TYPE_comment)
        str_append(str_sql, ",comment");
    if (opmask & RULE_TYPE_timeout)
        str_append(str_sql, ",timeout");
    if (opmask & RULE_TYPE_error_msg)
        str_append(str_sql, ",error_msg");
    if (opmask & RULE_TYPE_log)
        str_append(str_sql, ",log");
    if (opmask & RULE_TYPE_apply)
        str_append(str_sql, ",apply");
    return false;
}

my_ulonglong STDCALL mysql_affected_rows(MYSQL *mysql)
{
    return (mysql->affected_rows == ~(my_ulonglong) 0) ? 0 : mysql->affected_rows;
}

int proxy_config_cluster_is_local(THD* thd)
{
    int null_value;
    int ret; 
    char master_uuid[2048];

    strcpy(master_uuid, proxy_server_uuid);
    /* if the arkproxy_master_uuid is null, or not existed, 
     * then the node is local, else not */
    ret = get_user_var_str((char const *)"arkproxy_master_uuid", 
        (char*)&master_uuid, 2048, 0, &null_value);
    if (ret)
        return true;

    return false;
}

int proxy_skip_sync_config_cluster(THD* thd)
{
    int null_value;
    int ret; 
    char master_uuid[2048];
    int in = 0;
    char *p[20] = {0};
    char *inner_ptr = NULL;
    char *buf;
    char *buf_free;

    strcpy(master_uuid, proxy_server_uuid);
    /* get the master uuid, if the uuid is null, then it is master self
     * then replicate to others node with uuid  */
    ret = get_user_var_str((char const *)"arkproxy_master_uuid", 
        (char*)&master_uuid, 2048, 0, &null_value);
    if (!ret && !null_value)
    {
        buf = (char*)my_malloc(strlen(master_uuid), MY_ZEROFILL);
        strcpy(buf, master_uuid);
        buf_free = buf;
        while ((p[in] = strtok_r(buf, ",", &inner_ptr)) != NULL)
        {
            /* if the uuid have existed, then skip it */
            if (!strcasecmp(p[in], proxy_server_uuid))
            {
                return true;
            }

            in++;
            buf = NULL;
        }
        my_free(buf_free);
    }

    return false;
}

int proxy_update_rule(THD* thd, config_element_t* ele)
{
    MYSQL* conn;
    str_t str_sql;
    char tmp[128];

    /* if the config center is existed, then store it there, otherwise
     * store to the mysql db of proxy backend server*/
    if (proxy_config_cluster_is_local(thd))
    {
        conn = proxy_get_config_server_connection(false);
        str_init(&str_sql);
        str_append(&str_sql, "UPDATE ");
        if (metadata_not_local())
        {
            /*TODO need to check the validation of proxy_namespace, 
             * only include number and character */
            str_append(&str_sql, proxy_namespace);
            str_append(&str_sql, ".proxy_rules");
        }
        else
        {
            str_append(&str_sql, "mysql.proxy_rules");
        }

        str_append(&str_sql, " SET ");
        
        rule_get_update_set_values(&str_sql, ele->rule, ele->rule->op_mask);
        str_append(&str_sql, " WHERE rule_id = ");
        sprintf(tmp, "%d", ele->rule->rule_id);
        str_append(&str_sql, tmp);

        if (mysql_real_query(conn, str_get(&str_sql), str_get_len(&str_sql)))
        {
            my_error(ER_CONFIG_ERROR, MYF(0), mysql_error(conn));
            str_deinit(&str_sql);
            mysql_close(conn);
            return true;
        }

        if (mysql_affected_rows(conn) == 0)
        {
            char tmp[512];
            sprintf(tmp, "Rule with id: %d not existed or not updated", ele->rule->rule_id);
            my_error(ER_CONFIG_ERROR, MYF(0), tmp);
            str_deinit(&str_sql);
            mysql_close(conn);
            return true;
        }

        mysql_close(conn);
    }
    return false;
}

int proxy_delete_rule(THD* thd, config_element_t* ele, int all)
{
    MYSQL* conn;
    str_t str_sql;
    char tmp[128];

    /* if the config center is existed, then store it there, otherwise
     * store to the mysql db of proxy backend server*/
    if (proxy_config_cluster_is_local(thd))
    {
        conn = proxy_get_config_server_connection(false);
        str_init(&str_sql);
        str_append(&str_sql, "DELETE FROM ");
        if (metadata_not_local())
        {
            /*TODO need to check the validation of proxy_namespace, 
             * only include number and character */
            str_append(&str_sql, proxy_namespace);
            str_append(&str_sql, ".proxy_rules");
        }
        else
        {
            str_append(&str_sql, "mysql.proxy_rules");
        }

        if (all == false)
        {
            str_append(&str_sql, " WHERE rule_id = ");
            sprintf(tmp, "%d", ele->rule->rule_id);
            str_append(&str_sql, tmp);
        }

        if (mysql_real_query(conn, str_get(&str_sql), str_get_len(&str_sql)))
        {
            my_error(ER_CONFIG_ERROR, MYF(0), mysql_error(conn));
            str_deinit(&str_sql);
            mysql_close(conn);
            return true;
        }

        if (mysql_affected_rows(conn) == 0)
        {
            char tmp[512];
            sprintf(tmp, "Rule with id: %d not existed", ele->rule->rule_id);
            my_error(ER_CONFIG_ERROR, MYF(0), tmp);
            str_deinit(&str_sql);
            mysql_close(conn);
            return true;
        }

        mysql_close(conn);
    }
    return false;
}

int proxy_add_rule(THD* thd, config_element_t* ele)
{
    MYSQL* conn;
    str_t str_sql;

    /* if the config center is existed, then store it there, otherwise
     * store to the mysql db of proxy backend server*/
    if (proxy_config_cluster_is_local(thd))
    {
        conn = proxy_get_config_server_connection(false);
        str_init(&str_sql);
        str_append(&str_sql, "INSERT INTO ");
        if (metadata_not_local())
        {
            /*TODO need to check the validation of proxy_namespace, 
             * only include number and character */
            str_append(&str_sql, proxy_namespace);
            str_append(&str_sql, ".proxy_rules");
        }
        else
        {
            str_append(&str_sql, "mysql.proxy_rules");
        }

        str_append(&str_sql, "(");
        rule_get_insert_field(&str_sql, ele->rule->op_mask);
        str_append(&str_sql, ") values");
        str_append(&str_sql, "(");
        rule_get_insert_values(&str_sql, ele->rule, ele->rule->op_mask);
        str_append(&str_sql, ")");

        if (mysql_real_query(conn, str_get(&str_sql), str_get_len(&str_sql)))
        {
            if (mysql_errno(conn) == ER_DUP_ENTRY)
            {
                char tmp[512];
                sprintf(tmp, "Rule with id: %d have existed", ele->rule->rule_id);
                my_error(ER_CONFIG_ERROR, MYF(0), tmp);
                str_deinit(&str_sql);
                mysql_close(conn);
                return true;
            }

            my_error(ER_CONFIG_ERROR, MYF(0), mysql_error(conn));
            str_deinit(&str_sql);
            mysql_close(conn);
            return true;
        }

        mysql_close(conn);
    }

    return false;
}

int proxy_onoffline_route(THD* thd, config_element_t* ele)
{
    proxy_router_t* router;
    proxy_servers_t* server;

    server = LIST_GET_FIRST(global_proxy_config.server_lst);
    while (server)
    {
        if (!strcasecmp(server->server_name, ele->server_name))
            break;

        server = LIST_GET_NEXT(link, server);
    }

    /* set to online */
    if (thd->lex->table_count == true)
    {
        if (ele->type == ROUTER_TYPE_RO)
            server->noread_routed = false;
        else
            server->nowrite_routed = false;
    }
    else 
    {
        if (ele->type == ROUTER_TYPE_RW)
            server->nowrite_routed = true;
        else
            server->noread_routed = true;
    }

    global_proxy_config.config_version++;
    return false;
}

int proxy_delete_route(THD* thd, config_element_t* ele)
{
    proxy_server_t* server_node;
    proxy_server_t* server_next;
    proxy_servers_t* server;

    if (ele->type == ROUTER_TYPE_RO)
    {
        server_node = LIST_GET_FIRST(global_proxy_config.ro_server_lst);
        while (server_node)
        {
            server_next = LIST_GET_NEXT(link, server_node);
            server = server_node->server;
            if (!strcmp(ele->server_name, server->server_name))
            {
                LIST_REMOVE(link, global_proxy_config.ro_server_lst, server_node);
                my_free(server_node);
            }
            server_node = server_next;
        }
    }

    if (ele->type == ROUTER_TYPE_RW)
    {
        server_node = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
        while (server_node)
        {
            server_next = LIST_GET_NEXT(link, server_node);
            server = server_node->server;
            if (!strcmp(ele->server_name, server->server_name))
            {
                LIST_REMOVE(link, global_proxy_config.rw_server_lst, server_node);
                my_free(server_node);
            }
            server_node = server_next;
        }
    }

    global_proxy_config.config_version++;
    return false;
}

int proxy_add_route(THD* thd, config_element_t* ele)
{
    proxy_router_t* router;
    proxy_servers_t* server;

    server = LIST_GET_FIRST(global_proxy_config.server_lst);
    while (server)
    {
        if (!strcasecmp(server->server_name, ele->server_name))
            break;

        server = LIST_GET_NEXT(link, server);
    }

    router = LIST_GET_FIRST(global_proxy_config.router_lst);
    while (router)
    {
        if (ele->type == ROUTER_TYPE_RO && router->router_type == ROUTER_TYPE_RO)
        {
            proxy_server_t* server2 =
                (proxy_server_t*)my_malloc(sizeof(proxy_server_t), MY_ZEROFILL);
            server2->server = server;
            LIST_ADD_LAST(link, global_proxy_config.ro_server_lst, server2);
            server2->route = router;
            server2->build_connection = true;
            server->routed = true;
            break;
        }
        else if (ele->type == ROUTER_TYPE_RW && router->router_type == ROUTER_TYPE_RW)
        {
            proxy_server_t* server2 =
                (proxy_server_t*)my_malloc(sizeof(proxy_server_t), MY_ZEROFILL);
            server2->server = server;
            LIST_ADD_LAST(link, global_proxy_config.rw_server_lst, server2);
            server2->route = router;
            server2->build_connection = true;
            server->routed = true;
            break;
        }

        router = LIST_GET_NEXT(link, router);
    }

    if (router == NULL)
    {
        proxy_server_t* server2 =
            (proxy_server_t*)my_malloc(sizeof(proxy_server_t), MY_ZEROFILL);
        server2->server = server;
        if (ele->type == ROUTER_TYPE_RO)
            LIST_ADD_LAST(link, global_proxy_config.ro_server_lst, server2);
        else if (ele->type == ROUTER_TYPE_RW)
            LIST_ADD_LAST(link, global_proxy_config.rw_server_lst, server2);

        server2->route = router;
        server2->build_connection = true;
        server->routed = true;
    }

    global_proxy_config.config_version++;
    return false;
}

int proxy_add_server(THD* thd, config_element_t* ele)
{
    proxy_router_t* router;
    proxy_servers_t* server;

    server = LIST_GET_FIRST(global_proxy_config.server_lst);
    while (server)
    {
        if (!strcasecmp(server->server_name, ele->server_name))
        {
            char tmp[512];
            sprintf(tmp, "There have existed the server named '%s'", ele->server_name);
            my_error(ER_CONFIG_ERROR, MYF(0), tmp);
            return true;
        }

        server = LIST_GET_NEXT(link, server);
    }

    server = (proxy_servers_t*)my_malloc(sizeof(proxy_servers_t), MY_ZEROFILL);
    strcpy(server->server_name, ele->server_name);
    strcpy(server->backend_host, ele->hostname);
    server->backend_port = ele->port;
    server->weight = ele->weight;
    server->current_weight = server->weight;
    server->max_slave_lag = ele->max_connections;
    server->server_status = SERVER_STATUS_OFFLINE;
    server->reconnect = true;
    LIST_ADD_LAST(link, global_proxy_config.server_lst, server);

    router = LIST_GET_FIRST(global_proxy_config.router_lst);
    while (router)
    {
        if (ele->type == ROUTER_TYPE_RO && router->router_type == ROUTER_TYPE_RO)
        {
            proxy_server_t* server2 =
                (proxy_server_t*)my_malloc(sizeof(proxy_server_t), MY_ZEROFILL);
            server2->server = server;
            LIST_ADD_LAST(link, global_proxy_config.ro_server_lst, server2);
            server2->route = router;
            server->routed = true;
            break;
        }
        else if (ele->type == ROUTER_TYPE_RW && router->router_type == ROUTER_TYPE_RW)
        {
            proxy_server_t* server2 =
                (proxy_server_t*)my_malloc(sizeof(proxy_server_t), MY_ZEROFILL);
            server2->server = server;
            LIST_ADD_LAST(link, global_proxy_config.rw_server_lst, server2);
            server2->route = router;
            server->routed = true;
            break;
        }

        router = LIST_GET_NEXT(link, router);
    }

    if (router == NULL)
    {
        proxy_server_t* server2 =
            (proxy_server_t*)my_malloc(sizeof(proxy_server_t), MY_ZEROFILL);
        server2->server = server;
        if (ele->type == ROUTER_TYPE_RO)
            LIST_ADD_LAST(link, global_proxy_config.ro_server_lst, server2);
        else if (ele->type == ROUTER_TYPE_RW)
            LIST_ADD_LAST(link, global_proxy_config.rw_server_lst, server2);

        server2->route = router;
        server->routed = true;
    }

    global_proxy_config.config_version++;
    return false;
}

int proxy_delete_server(THD* thd, config_element_t* ele)
{
    proxy_server_t* server_node;
    proxy_servers_t* server;
    int entered = false;
    proxy_server_t* server_next;
    char tmp[512];

    server = LIST_GET_FIRST(global_proxy_config.server_lst);
    while (server)
    {
        if (!strcasecmp(server->server_name, ele->server_name))
        {
            if (server->server_status == SERVER_STATUS_ONLINE)
            {
                sprintf(tmp, "The status of '%s' is ONLINE, please set it OFFLINE first", ele->server_name);
                my_error(ER_CONFIG_ERROR, MYF(0), tmp);
                return true;
            }
            LIST_REMOVE(link, global_proxy_config.server_lst, server);
            my_free(server);
            break;
        }
        server = LIST_GET_NEXT(link, server);
    }

    if (server == NULL)
    {
        sprintf(tmp, "Server '%s' has not existed", ele->server_name);
        my_error(ER_CONFIG_ERROR, MYF(0), tmp);
        return true;
    }

    server_node = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
    while (server_node)
    {
        server_next = LIST_GET_NEXT(link, server_node);
        server = server_node->server;
        if (!strcmp(ele->server_name, server->server_name))
        {
            LIST_REMOVE(link, global_proxy_config.rw_server_lst, server_node);
            my_free(server_node);
        }
        server_node = server_next;
    }

    server_node = LIST_GET_FIRST(global_proxy_config.ro_server_lst);
    while (server_node)
    {
        server_next = LIST_GET_NEXT(link, server_node);
        server = server_node->server;
        if (!strcmp(ele->server_name, server->server_name))
        {
            LIST_REMOVE(link, global_proxy_config.ro_server_lst, server_node);
            my_free(server_node);
        }
        server_node = server_next;
    }

    global_proxy_config.config_version++;
    return false;
}

int proxy_set_server_status(THD* thd, config_element_t* ele)
{
    proxy_server_t* server;
    int online_sum = 0;

    /* set the write node first , if the server is can not been set offline
     * or online, then report error, but read is not report */
    if (!proxy_multi_write_mode)
    {
        server = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
        while (server)
        {
            if (server->server->server_status == SERVER_STATUS_ONLINE)
                online_sum ++;

            server = LIST_GET_NEXT(link, server);
        }
    }

    server = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
    while (server)
    {
        if (!strcasecmp(ele->server_name, server->server->server_name))
        {
//            /* if the server is online already, then omit it */
//            if (server->server->server_status == ele->sub_command)
//                break;

            if (!proxy_multi_write_mode)
            {
                if (ele->sub_command == SERVER_STATUS_ONLINE)
                {
                    /* can been set to online when there is not online node */
                    if (online_sum == 0)
                    {
                        server->server->server_status = SERVER_STATUS_ONLINE;
                        server->server->reconnect = true;
                    }
                    else if (online_sum >= 1)
                    {
                        /* set online for write offline server, but if there have one 
                         * online write node, and not the multi write mode, then report error */
                        my_error(ER_CONFIG_ERROR, MYF(0), "There can be up to one "
                            "online write node when proxy_multi_write_mode = OFF.");
                        return true;
                    }
                }
                else
                {
                    server->server->server_status = SERVER_STATUS_OFFLINE;
                }
            }
            else
            {
                /* multi write node mode */
                if (//server->server->server_status == SERVER_STATUS_OFFLINE &&
                    ele->sub_command == CFG_CMD_ONLINE_SERVER)
                {
                    server->server->reconnect = true;
                }

                server->server->server_status = ele->sub_command == CFG_CMD_OFFLINE_SERVER ?
                  SERVER_STATUS_OFFLINE : SERVER_STATUS_ONLINE;
            }

            break;
        }

        server = LIST_GET_NEXT(link, server);
    }

    server = LIST_GET_FIRST(global_proxy_config.ro_server_lst);
    while (server)
    {
        if (!strcasecmp(ele->server_name, server->server->server_name))
        {
            /* set offline for write online server */
            if (//server->server->server_status == SERVER_STATUS_OFFLINE &&
                ele->sub_command == CFG_CMD_ONLINE_SERVER)
            {
                server->server->reconnect = true;
            }

            server->server->server_status = ele->sub_command == CFG_CMD_OFFLINE_SERVER ?
              SERVER_STATUS_OFFLINE : SERVER_STATUS_ONLINE;

            break;
        }

        server = LIST_GET_NEXT(link, server);
    }

    global_proxy_config.config_version++;
    return false;
}

int proxy_set_user_config(THD* thd, config_element_t* ele)
{
    char key[256];
    int key_length;
    my_hash_value_type hash_value;
    proxy_user_t* proxy_user;
    MYSQL* conn;

    /* if this node is not master, and then not to 
     * update the backend config tables, only to update the cache */
    if (proxy_config_cluster_is_local(thd))
    {
        conn = proxy_get_config_server_connection(false);
        str_t str_sql;

        /* if the config center is existed, then store it there, otherwise
         * store to the mysql db of proxy backend server*/
        str_init(&str_sql);
        str_append(&str_sql, "INSERT INTO ");
        if (metadata_not_local())
        {
            /*TODO need to check the validation of proxy_namespace, 
             * only include number and character */
            str_append(&str_sql, proxy_namespace);
            str_append(&str_sql, ".proxy_user");
        }
        else
        {
            str_append(&str_sql, "mysql.proxy_user");
        }

        str_append(&str_sql, "(user_name, host, schema_name, max_connections) values");
        str_append(&str_sql, "(");
        sprintf(key, "\'%s\',", ele->username);
        str_append(&str_sql, key);
        sprintf(key, "\'%s\',", ele->hostname);
        str_append(&str_sql, key);
        sprintf(key, "\'%s\',", ele->dbname);
        str_append(&str_sql, key);
        sprintf(key, "%ld", ele->max_connections);
        str_append(&str_sql, key);
        str_append(&str_sql, ")");
        str_append(&str_sql, "ON DUPLICATE KEY UPDATE schema_name=");
        sprintf(key, "\'%s\',", ele->dbname);
        str_append(&str_sql, key);
        str_append(&str_sql, "max_connections=");
        sprintf(key, "%ld", ele->max_connections);
        str_append(&str_sql, key);

        if (mysql_real_query(conn, str_get(&str_sql), str_get_len(&str_sql)))
        {
            my_error(ER_CONFIG_ERROR, MYF(0), mysql_error(conn));
            str_deinit(&str_sql);
            mysql_close(conn);
            return true;
        }

        mysql_close(conn);
    }

    sprintf(key, "%s%s", (char*)ele->username, ele->hostname);
    key_length = strlen(key);
    hash_value= my_calc_hash(&global_proxy_config.proxy_user, (uchar*) key, key_length);

    proxy_user = (proxy_user_t*)my_hash_search_using_hash_value(&global_proxy_config.proxy_user,
                hash_value, (uchar*) key, key_length);
    if (proxy_user)
    {
        proxy_user->max_connections = ele->max_connections;
        strcpy(proxy_user->dbname, ele->dbname);
    }
    else
    {
        proxy_user = (proxy_user_t*)my_malloc(sizeof(proxy_user_t), MY_ZEROFILL);
        strcpy(proxy_user->user, ele->username);
        strcpy(proxy_user->host, ele->hostname);
        strcpy(proxy_user->dbname, ele->dbname);
        proxy_user->max_connections = ele->max_connections;
        if (my_hash_insert(&global_proxy_config.proxy_user, (uchar*) proxy_user))
            return true;
    }

    return false;
}

int proxy_config_delete(THD* thd)
{
    config_element_t* ele;
    config_element_t* ele_next;

    if (thd->config_cache == NULL)
        goto end;

    ele = LIST_GET_FIRST(thd->config_cache->config_lst);
    while (ele)
    {
        ele_next = LIST_GET_NEXT(link, ele);
        if (thd->lex->table_count == 0)
        {
            LIST_REMOVE(link, thd->config_cache->config_lst, ele);
            my_free(ele);
        }
        else if (ele->sequence == thd->lex->table_count)
        {
            LIST_REMOVE(link, thd->config_cache->config_lst, ele);
            my_free(ele);
            break;
        }

        ele = ele_next;
    }

end:
    my_ok(thd);
    return false;
}

int proxy_config_flush_low(THD* thd, config_element_t* ele)
{
    switch (ele->sub_command)
    {
    case CFG_CMD_ONLINE_SERVER:
    case CFG_CMD_OFFLINE_SERVER:
        return proxy_set_server_status(thd, ele);
    case CFG_CMD_RELOAD:
        break;
    case CFG_ADD_ROUTE:
        proxy_add_route(thd, ele);
        break;
    case CFG_DELETE_ROUTE:
        proxy_delete_route(thd, ele);
        break;
    case CFG_ONOFFLINE_ROUTE:
        proxy_onoffline_route(thd, ele);
        break;
    case CFG_ADD_SERVER:
        return proxy_add_server(thd, ele);
    case CFG_DELETE_SERVER:
       return proxy_delete_server(thd, ele);
    /* update the config tables in proxy_namespace */
    case CFG_RULE_ADD:
        return proxy_add_rule(thd, ele);
    case CFG_RULE_DELETE:
        return proxy_delete_rule(thd, ele, false);
    case CFG_RULE_DELETE_ALL:
        return proxy_delete_rule(thd, ele, true);
    case CFG_RULE_UPDATE:
        return proxy_update_rule(thd, ele);
    case CFG_CMD_USER_SET:
        return proxy_set_user_config(thd, ele);
    default:
        break;
    }

    return false;
}

int proxy_sync_config_cluster(THD* thd, char* command)
{
    config_element_t* ele;
    address_t* addr;
    address_t* addr_next;
    backend_conn_t conn;
    str_t msg;
    int null_value;
    int ret; 
    char master_uuid[2048];
    char set_uuid[2048];

    strcpy(master_uuid, proxy_server_uuid);
    /* get the master uuid, if the uuid is null, then it is master self
     * then replicate to others node with uuid  */
    get_user_var_str((char const *)"arkproxy_master_uuid", 
        (char*)&master_uuid, 2048, 0, &null_value);

    str_init(&msg);
    addr = LIST_GET_FIRST(global_proxy_config.incoming_lst);
    while (addr)
    {
        addr_next = LIST_GET_NEXT(link, addr);

        conn.set_mysql(NULL);
        init_backend_conn_info(&conn, addr->hostname, shell_username,
                               shell_password, addr->port);
        /* if the node can not connect, then skip it */
        MYSQL *mysql = get_backend_connection(thd, &conn);
        if (!mysql)
        {
            char tmp[1024];
            sprintf(tmp, "connect proxy cluster node failed: %s:%ld, \'%s\'",
                addr->hostname, addr->port, thd->get_stmt_da()->message());
            thd->clear_error();
            sql_print_warning(tmp);
            str_append(&msg, tmp);
            str_append(&msg, "\n");
            addr = addr_next;
            continue;
        }

        if (!strcasecmp(master_uuid, proxy_server_uuid))
            sprintf(set_uuid, "set @arkproxy_master_uuid = '%s'", master_uuid);
        else
            sprintf(set_uuid, "set @arkproxy_master_uuid = '%s,%s'", master_uuid, proxy_server_uuid);

        if (mysql_real_query(mysql, set_uuid, strlen(set_uuid)))
        {
            char tmp[1024];
            sprintf(tmp, "set var on proxy cluster node failed: %s:%ld, %s, command: \'%s\'",
                addr->hostname, addr->port, mysql_error(mysql), set_uuid);
            sql_print_warning(tmp);
            str_append(&msg, tmp);
            str_append(&msg, "\n");
            addr = addr_next;
            continue;
        }

        ele = thd->config_cache ? LIST_GET_FIRST(thd->config_cache->sync_lst) : NULL;
        while (ele)
        {
            if (mysql_real_query(mysql, str_get(&ele->config_command), 
                  str_get_len(&ele->config_command)))
            {
                char tmp[1024];
                sprintf(tmp, "update proxy cluster node failed: %s:%ld, %s, command: \'%s\'",
                    addr->hostname, addr->port, mysql_error(mysql),
                    str_get(&ele->config_command));
                sql_print_warning(tmp);
                str_append(&msg, tmp);
                str_append(&msg, "\n");
                break;
            }

            ele = LIST_GET_NEXT(link, ele);
        }

        /* if the config update all, then flush it */
        if (!ele && mysql_real_query(mysql, command, strlen(command)))
        {
            char tmp[1024];
            sprintf(tmp, "update proxy cluster node failed: %s:%ld, %s, command: \'%s\'",
                addr->hostname, addr->port, mysql_error(mysql), command);
            sql_print_warning(tmp);
            str_append(&msg, tmp);
            str_append(&msg, "\n");
        }

        mysql_close(mysql);
        addr = addr_next;
    }

    while (thd->config_cache && LIST_GET_LEN(thd->config_cache->sync_lst))
    {
        ele = LIST_GET_FIRST(thd->config_cache->sync_lst);
        LIST_REMOVE(link, thd->config_cache->sync_lst, ele);
        str_deinit(&ele->config_command);
        my_free(ele);
    }

    if (str_get_len(&msg) > 0)
    {
        my_error(ER_CONFIG_SYNC_ERROR, MYF(0), str_get(&msg));
        str_deinit(&msg);
        return true;
    }

    return false;
}

int proxy_config_flush(THD* thd)
{
    config_element_t* ele;
    config_element_t* ele_next;
    int rule_config = false;

    if (global_proxy_config.setting)
    {
        my_error(ER_CONFIG_IS_SETTING, MYF(0));
        return true;
    }

    if (thd->config_cache == NULL)
        goto end;

    if (proxy_skip_sync_config_cluster(thd))
        goto end;

    mysql_mutex_lock(&global_proxy_config.config_lock);
    LIST_INIT(thd->config_cache->sync_lst);
    global_proxy_config.setting = true;
    ele = LIST_GET_FIRST(thd->config_cache->config_lst);
    while (ele)
    {
        ele_next = LIST_GET_NEXT(link, ele);
        if (proxy_config_flush_low(thd, ele))
        {
            global_proxy_config.setting = false;
            mysql_mutex_unlock(&global_proxy_config.config_lock);
            return true;
        }

        if (ele->sub_command == CFG_RULE_DELETE ||
            ele->sub_command == CFG_RULE_DELETE_ALL ||
            ele->sub_command == CFG_RULE_UPDATE ||
            ele->sub_command == CFG_RULE_ADD)
        {
            rule_config = true;
        }

        LIST_REMOVE(link, thd->config_cache->config_lst, ele);
        LIST_ADD_LAST(link, thd->config_cache->sync_lst, ele);
        ele = ele_next;
    }

    if (rule_config)
        proxy_load_rules();

    global_proxy_config.setting = false;
    mysql_mutex_unlock(&global_proxy_config.config_lock);

    if (proxy_sync_config_cluster(thd, (char*)"config flush"))
    {
        my_free(thd->config_cache);
        thd->config_cache = NULL;
        return true;
    }

    my_free(thd->config_cache);
    thd->config_cache = NULL;

end:
    my_ok(thd);
    return false;
}

int
proxy_set_threads_reload(THD* thd)
{
    if (proxy_skip_sync_config_cluster(thd))
    {
        my_ok(thd);
        return false;
    }

    if (proxy_config_reload())
    {
        return true;
    }
  
    mysql_mutex_lock(&LOCK_thread_count); // For unlink from list
    I_List_iterator<THD> it(threads);
    THD *tmp;
    tmp = first_global_thread();
    while (tmp)
    {
        tmp->reconfig = true;
        /* TODO: free current connections 
 	* will free in the reconnection when new sql arrived */
        //close_ark_conn(tmp);
        tmp = next_global_thread(tmp);
    }

    mysql_mutex_unlock(&LOCK_thread_count);

    if (proxy_sync_config_cluster(thd, (char*)"config reload"))
        return true;

    my_ok(thd);
    return false;
}

void
proxy_get_user_config_attribute(str_t* str, ulong max_connections, char* dbname)
{
    char value[100];
    str_init(str);
    str_append(str, "{");
    str_append(str, "\"max_connections\":");
    sprintf(value, "\"%ld\", ", max_connections);
    str_append(str, value);
    str_append(str, "\"databases\":");
    str_append(str, "\"");
    str_append(str, dbname);
    str_append(str, "\"");
    str_append(str, "}");
}

void proxy_show_user_config_list(THD *thd)
{
    List<Item> field_list;
    Protocol *protocol= thd->protocol;
    MEM_ROOT *mem_root= thd->mem_root;
    proxy_user_t* proxy_user;

    DBUG_ENTER("proxy_show_user_config_list");
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Id", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "User", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Host",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Schema_Name",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                          Item_return_int(thd, "Max_Connections", 7, MYSQL_TYPE_LONG),
                         mem_root);
    field_list.push_back(new (mem_root)
                          Item_return_int(thd, "Current_Connections", 7, MYSQL_TYPE_LONG),
                         mem_root);
    if (protocol->send_result_set_metadata(&field_list,
                                           Protocol::SEND_NUM_ROWS |
                                           Protocol::SEND_EOF))
        DBUG_VOID_RETURN;
    int id = 0;
    for (uint i=0; i < global_proxy_config.proxy_user.records; i++)
    {
        protocol->prepare_for_resend();
        protocol->store((ulonglong) ++id);
        proxy_user = (proxy_user_t*)my_hash_element(&global_proxy_config.proxy_user, i);
        protocol->store(proxy_user->user, system_charset_info);
        protocol->store(proxy_user->host, system_charset_info);
        protocol->store(proxy_user->dbname, system_charset_info);
        protocol->store_long(proxy_user->max_connections);
        protocol->store_long(proxy_user->conn_count);
        protocol->write();
    }

    my_eof(thd);
    DBUG_VOID_RETURN;
}

void proxy_show_config_cache(THD *thd)
{
    proxy_servers_t* server; 
    List<Item> field_list;
    Protocol *protocol= thd->protocol;
    MEM_ROOT *mem_root= thd->mem_root;

    config_element_t* ele;

    DBUG_ENTER("proxy_show_config_cache");
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Id", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Config_Class", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Config_Name",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "New_Value",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Old_Value",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    if (protocol->send_result_set_metadata(&field_list,
                                           Protocol::SEND_NUM_ROWS |
                                           Protocol::SEND_EOF))
        DBUG_VOID_RETURN;

    if (thd->config_cache != NULL)
    {
        ele = LIST_GET_FIRST(thd->config_cache->config_lst);
        while (ele)
        {
            protocol->prepare_for_resend();
            if (ele->sub_command == CFG_CMD_ONLINE_SERVER || 
                ele->sub_command == CFG_CMD_OFFLINE_SERVER)
            {
                protocol->store((ulonglong) ele->sequence);
                protocol->store("SERVER_STATUS", system_charset_info);
                protocol->store(ele->server_name, system_charset_info);
                protocol->store(ele->sub_command == SERVER_STATUS_ONLINE ? 
                    "ONLINE" : "OFFLINE", system_charset_info);
                mysql_mutex_lock(&global_proxy_config.config_lock);
                server = LIST_GET_FIRST(global_proxy_config.server_lst);
                while (server)
                {
                    if (!strcasecmp(server->server_name, ele->server_name))
                    {
                        protocol->store(server->server_status == SERVER_STATUS_ONLINE ? 
                            "ONLINE" : "OFFLINE", system_charset_info);
                        break;
                    }
                    server = LIST_GET_NEXT(link, server);
                }
                if (!server)
                {
                    protocol->store("OFFLINE", system_charset_info);
                }
                mysql_mutex_unlock(&global_proxy_config.config_lock);

                protocol->write();
            }
            else if (ele->sub_command == CFG_CMD_USER_SET)
            {
                str_t json;
                int found = false;
                proxy_user_t* proxy_user;
                protocol->store((ulonglong) ele->sequence);
                protocol->store("USER_CONFIG", system_charset_info);
                char username[200];
                sprintf(username, "%s@%s", ele->username, ele->hostname);
                protocol->store(username, system_charset_info);
                proxy_get_user_config_attribute(&json, ele->max_connections, ele->dbname);
                protocol->store(str_get(&json), system_charset_info);

                for (uint i=0; i < global_proxy_config.proxy_user.records; i++)
                {
                    proxy_user = (proxy_user_t*)my_hash_element(&global_proxy_config.proxy_user, i);
                    if (!strcasecmp(proxy_user->user, ele->username)&&
                        !strcasecmp(proxy_user->host, ele->hostname))
                    {
                        proxy_get_user_config_attribute(&json, proxy_user->max_connections, 
                            proxy_user->dbname);
                        protocol->store(str_get(&json), system_charset_info);
                        found = true;
                    }
                }

                if (!found)
                    protocol->store("NULL", system_charset_info);

                protocol->write();
            }
            else if (ele->sub_command == CFG_ADD_SERVER)
            {
                protocol->store((ulonglong) ele->sequence);
                protocol->store("ADD_SERVER", system_charset_info);
                protocol->store(ele->server_name, system_charset_info);
                char tmp[1024];

                str_t json;
                str_t *str = &json;
                str_init(str);
                str_append(str, "{");
                str_append(str, "\"server_name\":");
                str_append(str, "\"");
                str_append(str, ele->server_name);
                str_append(str, "\"");
                str_append(str, ",");
                str_append(str, "\"route_type\":");
                str_append(str, "\"");
                if (ele->type == ROUTER_TYPE_RO)
                    str_append(str, "Read");
                else
                    str_append(str, "Write");
                str_append(str, "\"");
                str_append(str, ",");
                str_append(str, "\"host_name\":");
                str_append(str, "\"");
                str_append(str, ele->hostname);
                str_append(str, "\"");
                str_append(str, ",");
                str_append(str, "\"port\":");
                str_append(str, "\"");
                sprintf(tmp, "%ld", ele->port);
                str_append(str, tmp);
                str_append(str, "\"");
                str_append(str, ",");
                str_append(str, "\"max_lag\":");
                str_append(str, "\"");
                sprintf(tmp, "%ld", ele->max_connections);
                str_append(str, tmp);
                str_append(str, "\"");
                str_append(str, ",");
                str_append(str, "\"weight\":");
                str_append(str, "\"");
                sprintf(tmp, "%ld", ele->weight);
                str_append(str, tmp);
                str_append(str, "\"");
                str_append(str, "}");
                protocol->store(str_get(str), str_get_len(str), system_charset_info);
                protocol->store("NULL", system_charset_info);
                protocol->write();

            }
            else if (ele->sub_command == CFG_ADD_ROUTE)
            {
                protocol->store((ulonglong) ele->sequence);
                protocol->store("ADD_ROUTE", system_charset_info);
                protocol->store(ele->server_name, system_charset_info);
                protocol->store("NULL", system_charset_info);
                protocol->store(str_get(&ele->config_command), 
                    str_get_len(&ele->config_command), system_charset_info);
                protocol->write();
            }
            else if (ele->sub_command == CFG_DELETE_ROUTE)
            {
                protocol->store((ulonglong) ele->sequence);
                protocol->store("DELETE_ROUTE", system_charset_info);
                protocol->store(ele->server_name, system_charset_info);
                protocol->store("NULL", system_charset_info);
                protocol->store(str_get(&ele->config_command), 
                    str_get_len(&ele->config_command), system_charset_info);
                protocol->write();
            }
            else if (ele->sub_command == CFG_ONOFFLINE_ROUTE)
            {
                protocol->store((ulonglong) ele->sequence);
                protocol->store("ON or OFF ROUTE", system_charset_info);
                protocol->store(ele->server_name, system_charset_info);
                protocol->store("NULL", system_charset_info);
                protocol->store(str_get(&ele->config_command), 
                    str_get_len(&ele->config_command), system_charset_info);
                protocol->write();
            }
            else if (ele->sub_command == CFG_DELETE_SERVER)
            {
                protocol->store((ulonglong) ele->sequence);
                protocol->store("DELETE_SERVER", system_charset_info);
                protocol->store(ele->server_name, system_charset_info);
                protocol->store("NULL", system_charset_info);
                protocol->store(str_get(&ele->config_command), 
                    str_get_len(&ele->config_command), system_charset_info);
                protocol->write();
            }
            else if (ele->sub_command == CFG_RULE_ADD)
            {
                char username[200];
                protocol->store((ulonglong) ele->sequence);
                protocol->store("RULE_CONFIG", system_charset_info);
                sprintf(username, "%d", ele->rule->rule_id);
                protocol->store(username, system_charset_info);
                protocol->store(str_get(&ele->config_command), 
                    str_get_len(&ele->config_command), system_charset_info);
                protocol->store("NULL", system_charset_info);
                protocol->write();
            }
            else if (ele->sub_command == CFG_RULE_DELETE)
            {
                char username[200];
                protocol->store((ulonglong) ele->sequence);
                protocol->store("RULE_CONFIG", system_charset_info);
                sprintf(username, "%d", ele->rule->rule_id);
                protocol->store(username, system_charset_info);
                protocol->store("NULL", system_charset_info);
                protocol->store(str_get(&ele->config_command), 
                    str_get_len(&ele->config_command), system_charset_info);
                protocol->write();
            }
            else if (ele->sub_command == CFG_RULE_DELETE_ALL)
            {
                char username[200];
                protocol->store((ulonglong) ele->sequence);
                protocol->store("RULE_CONFIG", system_charset_info);
                protocol->store("ALL", system_charset_info);
                protocol->store("NULL", system_charset_info);
                protocol->store(str_get(&ele->config_command), 
                    str_get_len(&ele->config_command), system_charset_info);
                protocol->write();
            }
            else if (ele->sub_command == CFG_RULE_UPDATE)
            {
                char username[200];
                protocol->store((ulonglong) ele->sequence);
                protocol->store("RULE_CONFIG", system_charset_info);
                sprintf(username, "%d", ele->rule->rule_id);
                protocol->store(username, system_charset_info);
                protocol->store(str_get(&ele->config_command), 
                    str_get_len(&ele->config_command), system_charset_info);
                protocol->store("NULL", system_charset_info);
                protocol->write();
            }

            ele = LIST_GET_NEXT(link, ele);
        }
    }

    my_eof(thd);
    DBUG_VOID_RETURN;

}

void mysqld_list_backend_servers(THD *thd,const char *user, bool verbose)
{
    proxy_servers_t* server; 
    List<Item> field_list;
    Protocol *protocol= thd->protocol;
    MEM_ROOT *mem_root= thd->mem_root;

    DBUG_ENTER("mysqld_list_backend_servers");
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Id", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Name", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Host",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                          Item_return_int(thd, "Port", 7, MYSQL_TYPE_LONG),
                         mem_root);
    field_list.push_back(new (mem_root)
                          Item_return_int(thd, "Weight", 7, MYSQL_TYPE_LONG),
                         mem_root);
    field_list.push_back(new (mem_root)
                          Item_return_int(thd, "Max_Slave_Lag", 7, MYSQL_TYPE_LONG),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Routed", USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Status", USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Comments", USERNAME_CHAR_LENGTH),
                         mem_root);
    if (protocol->send_result_set_metadata(&field_list,
                                           Protocol::SEND_NUM_ROWS |
                                           Protocol::SEND_EOF))
        DBUG_VOID_RETURN;

    int id=0;
    mysql_mutex_lock(&global_proxy_config.config_lock);
    server = LIST_GET_FIRST(global_proxy_config.server_lst);
    while (server)
    {
        protocol->prepare_for_resend();
        protocol->store((ulonglong) ++id);
        protocol->store(server->server_name, system_charset_info);
        protocol->store(server->backend_host, system_charset_info);
        protocol->store_long(server->backend_port);
        protocol->store_long(server->weight);
        protocol->store_long(server->max_slave_lag);
        protocol->store(server->routed ? "Yes" : "No", system_charset_info);
        protocol->store(server->server_status == SERVER_STATUS_OFFLINE ? "OFFLINE" : "ONLINE", 
            system_charset_info);
        protocol->store(server->comments, system_charset_info);
        protocol->write();

        server = LIST_GET_NEXT(link, server);
    }
    mysql_mutex_unlock(&global_proxy_config.config_lock);

    my_eof(thd);
    DBUG_VOID_RETURN;
}

void mysqld_list_backend_routes(THD *thd,const char *user, bool verbose)
{
    proxy_servers_t* server; 
    proxy_servers_t* serverb; 
    proxy_server_t* server_node; 
    int entered = false;
    List<Item> field_list;
    Protocol *protocol= thd->protocol;
    MEM_ROOT *mem_root= thd->mem_root;

    DBUG_ENTER("mysqld_list_backend_servers");
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Id", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Name",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Host",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Port", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Route_Type",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Comments",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Routed",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    if (protocol->send_result_set_metadata(&field_list,
                                           Protocol::SEND_NUM_ROWS |
                                           Protocol::SEND_EOF))
        DBUG_VOID_RETURN;

    int id=0;
    mysql_mutex_lock(&global_proxy_config.config_lock);
    server_node = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
    while (server_node)
    {
        server = server_node->server;
        protocol->prepare_for_resend();
        protocol->store((ulonglong) ++id);
        protocol->store(server->server_name, system_charset_info);
        protocol->store(server->backend_host, system_charset_info);
        protocol->store((ulonglong) server->backend_port);
        if (!entered)
            protocol->store("Write", system_charset_info);
        else
            protocol->store("Read", system_charset_info);
        if (server_node->route)
            protocol->store(server_node->route->comments, system_charset_info);
        else
            protocol->store(NULL, system_charset_info);

        serverb = LIST_GET_FIRST(global_proxy_config.server_lst);
        while (serverb)
        {
            if (!strcasecmp(serverb->server_name, server->server_name))
            {
                if (serverb->nowrite_routed && !entered)
                    protocol->store("OFF", system_charset_info);
                else if (serverb->noread_routed && entered)
                    protocol->store("OFF", system_charset_info);
                else
                    protocol->store("ON", system_charset_info);
            }

            serverb = LIST_GET_NEXT(link, serverb);
        }

        server_node = LIST_GET_NEXT(link, server_node);
        if (server_node == NULL && !entered)
        {
            entered = true;
            server_node = LIST_GET_FIRST(global_proxy_config.ro_server_lst);
        }
        protocol->write();
    }
    mysql_mutex_unlock(&global_proxy_config.config_lock);

    mysql_mutex_lock(&global_proxy_config.config_lock);
    server = LIST_GET_FIRST(global_proxy_config.server_lst);
    while (server)
    {
        if (!server->routed)
        {
            protocol->prepare_for_resend();
            protocol->store((ulonglong) ++id);
            protocol->store(server->server_name, system_charset_info);
            protocol->store(server->backend_host, system_charset_info);
            protocol->store((ulonglong) server->backend_port);
            protocol->store("No Routed", system_charset_info);
            protocol->store(NULL, system_charset_info);
            protocol->store("OFF", system_charset_info);
            protocol->write();
        }

        server = LIST_GET_NEXT(link, server);
    }

    mysql_mutex_unlock(&global_proxy_config.config_lock);
    my_eof(thd);
    DBUG_VOID_RETURN;
}

void number_to_timestr(int second, int micro, char* buffer)
{
    MYSQL_TIME ltime;
    struct tm tmp_tm;
    char buff[MAX_DATE_STRING_REP_LENGTH];
    time_t tmp_t= (time_t)second;
    localtime_r(&tmp_t, &tmp_tm);
    localtime_to_TIME(&ltime, &tmp_tm);
    ltime.time_type= MYSQL_TIMESTAMP_DATETIME;
    ltime.second_part = micro;
    if (!micro)
        my_datetime_to_str((const MYSQL_TIME*)&ltime, buffer, 0);
    else
    {
        my_datetime_to_str(&ltime, buff, 0);
        sprintf(buffer, "%s.%d", buff, micro);
    }
}

void mysqld_list_connections(THD *thd,const char *user, bool verbose)
{
    THD* tmp;
    Item *field;
    List<Item> field_list;
    Protocol *protocol= thd->protocol;
    MEM_ROOT *mem_root= thd->mem_root;
    DBUG_ENTER("mysqld_list_backend_connections");

    field_list.push_back(new (mem_root)
                         Item_int(thd, "Parent_Id", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "User",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Host",
                                           LIST_PROCESS_HOST_LEN),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "MD5_User",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);

    field_list.push_back(field= new (mem_root)
                         Item_return_int(thd, "Env_Version", 7, MYSQL_TYPE_LONG),
                         mem_root);
    field_list.push_back(field= new (mem_root)
                         Item_return_int(thd, "send_queue_len", 7, MYSQL_TYPE_LONG),
                         mem_root);
    field_list.push_back(field= new (mem_root)
                         Item_return_int(thd, "recv_queue_len", 7, MYSQL_TYPE_LONG),
                         mem_root);
    field_list.push_back(field= new (mem_root)
                         Item_return_int(thd, "cond_signal_count", 7, MYSQL_TYPE_LONG),
                         mem_root);
    field_list.push_back(field= new (mem_root)
                         Item_return_int(thd, "cond_wait_count", 7, MYSQL_TYPE_LONG),
                         mem_root);
    if (protocol->send_result_set_metadata(&field_list,
                                           Protocol::SEND_NUM_ROWS |
                                           Protocol::SEND_EOF))
        DBUG_VOID_RETURN;

    if (thd->killed)
        DBUG_VOID_RETURN;

    mysql_mutex_lock(&LOCK_thread_count); // For unlink from list

    tmp = first_global_thread();
    while (tmp)
    {
        Security_context *tmp_sctx= tmp->security_ctx;
        if ((tmp->vio_ok() || tmp->system_thread) &&
            (!user || (!tmp->system_thread &&
            tmp_sctx->user && !strcmp(tmp_sctx->user, user))))
        {
            backend_conn_t* conn;
            int i=0;
            protocol->prepare_for_resend();
            conn = tmp->write_conn[i];
            net_queue_t* send_queue;
            net_queue_t* recv_queue;
            send_queue = &tmp->queue_out;
            recv_queue = &tmp->queue_in;

            const char* ip_or_host = tmp_sctx->host_or_ip;
            protocol->store((ulonglong) tmp->thread_id);
            protocol->store(tmp_sctx->external_user, system_charset_info);
            protocol->store(ip_or_host, system_charset_info);
            protocol->store(tmp_sctx->user, system_charset_info);
            protocol->store(tmp->env_version);
            int queue_length;
            if (send_queue->enqueue_index != send_queue->dequeue_index)
                queue_length = ((int)((send_queue->enqueue_index+ tmp->thd_proxy_send_queue_size - 
                        send_queue->dequeue_index) % tmp->thd_proxy_send_queue_size));
            else
                queue_length = 0;
            protocol->store(queue_length);
            if (recv_queue->enqueue_index != recv_queue->dequeue_index)
                queue_length = ((int)((recv_queue->enqueue_index+ tmp->thd_proxy_recv_queue_size - 
                        recv_queue->dequeue_index) % tmp->thd_proxy_recv_queue_size));
            else
                queue_length = 0;
            protocol->store(queue_length);
            protocol->store(tmp->cond_signal_count);
            protocol->store(tmp->cond_wait_count);
            protocol->write();
        }
        tmp = next_global_thread(tmp);
    }
    mysql_mutex_unlock(&LOCK_thread_count);
    my_eof(thd);
    DBUG_VOID_RETURN;
}

void mysqld_show_proxy_status(THD *thd, const char *user, bool verbose) {
    THD* tmp;
    List<Item> field_list;
    Protocol *protocol= thd->protocol;
    MEM_ROOT *mem_root= thd->mem_root;
    DBUG_ENTER("mysqld_show_proxy_status");
    int64 queue_packet_buf_size = 0;
    int64 max_packet_buf_size = 0;
    int64 min_packet_buf_size = 0;
    field_list.push_back(new (mem_root) Item_int(thd, "Connection_Count", 0,
                                                 MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root) Item_int(thd, "Min_Con_Buffer_Mem", 0,
                                                 MY_INT64_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root) Item_int(thd, "Max_Con_Buffer_Mem", 0,
                                                 MY_INT64_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root) Item_int(thd, "Total_Packet_Mem", 0,
                                                 MY_INT64_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root) Item_int(thd, "Total_Buffer_Mem", 0,
                                                 MY_INT64_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(
        new (mem_root) Item_int(thd, "Total_Mem", 0, MY_INT64_NUM_DECIMAL_DIGITS),
        mem_root);
    if (protocol->send_result_set_metadata(&field_list,
                                           Protocol::SEND_NUM_ROWS |
                                           Protocol::SEND_EOF))
        DBUG_VOID_RETURN;

    if (thd->killed)
        DBUG_VOID_RETURN;

    mysql_mutex_lock(&LOCK_thread_count); // For unlink from list

    tmp = first_global_thread();
    int conn_count = 0;
    while (tmp)
    {
        Security_context *tmp_sctx= tmp->security_ctx;
        if ((tmp->vio_ok() || tmp->system_thread) &&
            (!user || (!tmp->system_thread &&
            tmp_sctx->user && !strcmp(tmp_sctx->user, user))))
        {
            if (tmp->write_conn_count > 0 || tmp->read_conn_count > 0) {
                queue_packet_buf_size += tmp->queue_packet_buf_size;
                if (tmp->queue_packet_buf_size > max_packet_buf_size) {
                    max_packet_buf_size = tmp->queue_packet_buf_size;
                }
                if (tmp->queue_packet_buf_size < min_packet_buf_size || min_packet_buf_size == 0) {
                    min_packet_buf_size = tmp->queue_packet_buf_size;
                }
                conn_count++;
            }
        }
        tmp = next_global_thread(tmp);
    }
    ulong packet_size = (proxy_send_queue_size + proxy_recv_queue_size) * sizeof(net_packet_t);
    protocol->prepare_for_resend();
    protocol->store(conn_count);
    protocol->store(min_packet_buf_size);
    protocol->store(max_packet_buf_size);
    protocol->store((ulonglong)packet_size * conn_count);
    protocol->store(queue_packet_buf_size);
    protocol->store((ulonglong)packet_size * conn_count + queue_packet_buf_size);
    protocol->write();
    mysql_mutex_unlock(&LOCK_thread_count);
    my_eof(thd);
    DBUG_VOID_RETURN;
}

void mysqld_show_proxy_conn_status(THD *thd,const char *user, bool verbose)
{
    THD* tmp;
    List<Item> field_list;
    Protocol *protocol= thd->protocol;
    MEM_ROOT *mem_root= thd->mem_root;
    DBUG_ENTER("mysqld_show_proxy_conn_status");

    field_list.push_back(new (mem_root) Item_int(thd, "Parent_Id", 0,
                                                 MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root) Item_int(thd, "Packet_Mem", 0,
                                                 MY_INT64_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root) Item_int(thd, "Buffer_Mem", 0,
                                                 MY_INT64_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root) Item_int(thd, "Send_Average_Buf_Len", 0,
                                                 MY_INT64_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root) Item_int(thd, "Recv_Average_Buf_Len", 0,
                                                 MY_INT64_NUM_DECIMAL_DIGITS),
                         mem_root);

    if (protocol->send_result_set_metadata(&field_list,
                                           Protocol::SEND_NUM_ROWS |
                                           Protocol::SEND_EOF))
        DBUG_VOID_RETURN;

    if (thd->killed)
        DBUG_VOID_RETURN;

    mysql_mutex_lock(&LOCK_thread_count); // For unlink from list

    tmp = first_global_thread();
    while (tmp)
    {
        Security_context *tmp_sctx= tmp->security_ctx;
        if ((tmp->vio_ok() || tmp->system_thread) &&
            (!user || (!tmp->system_thread && tmp_sctx->user &&
                       !strcmp(tmp_sctx->user, user)))) {
          if (tmp->write_conn_count > 0 || tmp->read_conn_count > 0) {
            protocol->prepare_for_resend();
            protocol->store((ulonglong)tmp->thread_id);
            protocol->store((ulonglong)(tmp->thd_proxy_send_queue_size +
                                        tmp->thd_proxy_recv_queue_size) *
                            sizeof(net_packet_t));
            protocol->store(tmp->queue_packet_buf_size);
            protocol->store(tmp->queue_out_packet_total_len /
                            tmp->thd_proxy_send_queue_size);
            protocol->store(tmp->queue_in_packet_total_len /
                            tmp->thd_proxy_recv_queue_size);
            protocol->write();
          }
        }
        tmp = next_global_thread(tmp);
    }
    mysql_mutex_unlock(&LOCK_thread_count);
    my_eof(thd);
    DBUG_VOID_RETURN;
}

void mysqld_list_backend_connections(THD *thd,const char *user, bool verbose)
{
    THD* tmp;
    Item *field;
    List<Item> field_list;
    Protocol *protocol= thd->protocol;
    MEM_ROOT *mem_root= thd->mem_root;
    DBUG_ENTER("mysqld_list_backend_connections");

    field_list.push_back(new (mem_root)
                         Item_int(thd, "Parent_Id", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "User",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Host",
                                           LIST_PROCESS_HOST_LEN),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "MD5_HASH",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Backend_Host",
                                           LIST_PROCESS_HOST_LEN),
                         mem_root);
    field_list.push_back(field= new (mem_root)
                         Item_return_int(thd, "Port", 7, MYSQL_TYPE_LONG),
                         mem_root);
    field_list.push_back(field=new (mem_root)
                         Item_empty_string(thd, "Db", NAME_CHAR_LEN),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Route_Type",
                                           LIST_PROCESS_HOST_LEN),
                         mem_root);
    field_list.push_back(field= new (mem_root)
                         Item_return_int(thd, "Wait_Timeout", 7, MYSQL_TYPE_LONG),
                         mem_root);
    field_list.push_back(field=new (mem_root)
                         Item_empty_string(thd, "Last_Heartbeat", NAME_CHAR_LEN),
                         mem_root);
    field_list.push_back(field=new (mem_root)
                         Item_empty_string(thd, "Last_Execution", NAME_CHAR_LEN),
                         mem_root);
    field_list.push_back(field= new (mem_root)
                         Item_return_int(thd, "Alived", 7, MYSQL_TYPE_LONG),
                         mem_root);
    field_list.push_back(field= new (mem_root)
                         Item_return_int(thd, "Env_Version", 7, MYSQL_TYPE_LONG),
                         mem_root);
    field_list.push_back(field= new (mem_root)
                         Item_return_int(thd, "Route_Count", 7, MYSQL_TYPE_LONGLONG),
                         mem_root);
    field_list.push_back(field= new (mem_root)
                         Item_return_int(thd, "Slave_Lag", 7, MYSQL_TYPE_LONG),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Running_SQL",
                                           TABLE_COMMENT_MAXLEN),
                         mem_root);
    if (protocol->send_result_set_metadata(&field_list,
                                           Protocol::SEND_NUM_ROWS |
                                           Protocol::SEND_EOF))
        DBUG_VOID_RETURN;

    if (thd->killed)
        DBUG_VOID_RETURN;

    mysql_mutex_lock(&LOCK_thread_count); // For unlink from list

    tmp = first_global_thread();
    while (tmp)
    {
        Security_context *tmp_sctx= tmp->security_ctx;
        if ((tmp->vio_ok() || tmp->system_thread) &&
            (!user || (!tmp->system_thread &&
            tmp_sctx->user && !strcmp(tmp_sctx->user, user))))
        {
            backend_conn_t* conn;
            int i=0;
            char buff[MAX_DATE_STRING_REP_LENGTH];
            for (i = 0; i< tmp->write_conn_count; i++)
            {
                protocol->prepare_for_resend();
                conn = tmp->write_conn[i];
                protocol->store((ulonglong) tmp->thread_id);
                protocol->store(conn->user, system_charset_info);
                protocol->store(conn->host, system_charset_info);
                protocol->store(conn->md5_hash, system_charset_info);
                if (conn->conn_inited() && conn->server)
                    protocol->store(conn->server->backend_host, system_charset_info);
                else
                    protocol->store("NULL", system_charset_info);

                protocol->store_long(conn->port);
                protocol->store(conn->db, system_charset_info);
                protocol->store("Write", system_charset_info);
                protocol->store_long(conn->wait_timeout);

                number_to_timestr(conn->last_heartbeat / 1000000, 
                    conn->last_heartbeat % 1000000, buff);
                protocol->store(buff, system_charset_info);

                number_to_timestr(conn->last_execute / 1000000, 
                    conn->last_execute % 1000000, buff);
                protocol->store(buff, system_charset_info);

                protocol->store_long(conn->conn_inited());
                protocol->store_long(conn->env_version);
                protocol->store_longlong(conn->qps, true);
                protocol->store_long(conn->slave_lag);
                protocol->store(conn->running_sql, system_charset_info);
                protocol->write();
            }

            for (i = 0; i< tmp->read_conn_count; i++)
            {
                protocol->prepare_for_resend();
                conn = tmp->read_conn[i];
                protocol->store((ulonglong) tmp->thread_id);
                protocol->store(conn->user, system_charset_info);
                protocol->store(conn->host, system_charset_info);
                protocol->store(conn->md5_hash, system_charset_info);
                if (conn->conn_inited() && conn->server)
                    protocol->store(conn->server->backend_host, system_charset_info);
                else
                    protocol->store("NULL", system_charset_info);
                protocol->store_long(conn->port);
                protocol->store(conn->db, system_charset_info);
                protocol->store("Read", system_charset_info);
                protocol->store_long(conn->wait_timeout);

                number_to_timestr(conn->last_heartbeat / 1000000, 
                    conn->last_heartbeat % 1000000, buff);
                protocol->store(buff, system_charset_info);

                number_to_timestr(conn->last_execute / 1000000, 
                    conn->last_execute % 1000000, buff);
                protocol->store(buff, system_charset_info);

                protocol->store_long(conn->conn_inited());
                protocol->store_long(conn->env_version);
                protocol->store_longlong(conn->qps, true);
                protocol->store_long(conn->slave_lag);
                protocol->store(conn->running_sql, system_charset_info);
                protocol->write();
            }
        }
        tmp = next_global_thread(tmp);
    }
    mysql_mutex_unlock(&LOCK_thread_count);
    my_eof(thd);
    DBUG_VOID_RETURN;
}

void proxy_trace_status(THD* thd)
{
    List<Item> field_list;
    Protocol *protocol= thd->protocol;
    MEM_ROOT *mem_root= thd->mem_root;
    
    DBUG_ENTER("proxy_trace_status");

    field_list.push_back(new (mem_root)
                         Item_int(thd, "Id", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Empty_Count", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Busy_Count", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Format_Count", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Reset_Count", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    
    if (protocol->send_result_set_metadata(&field_list,
                                           Protocol::SEND_NUM_ROWS |
                                           Protocol::SEND_EOF))
        DBUG_VOID_RETURN;
    
    int id=0;
    int empty_Count = 0;
    int busy_Count = 0;
    int format_Count = 0;
    int reset_Count = 0;
    mysql_mutex_lock(&global_trace_cache->trace_lock);
    if (trace_digest_inited)
    {
        for (int i = 0; i < global_trace_cache->trace_cache_size; ++i)
        {
            trace_queue_t* queue = &global_trace_cache->trace_queue[i];
            if (queue->trace_state == TRACE_STATE_EMPTY)
                empty_Count++;
            else if (queue->trace_state == TRACE_STATE_BUSY)
                busy_Count++;
            else if (queue->trace_state == TRACE_STATE_FORMAT)
                format_Count++;
            else if (queue->trace_state == TRACE_STATE_RESET)
                reset_Count++;
        }
        protocol->prepare_for_resend();
        protocol->store((ulonglong)++id);
        protocol->store((ulonglong)empty_Count);
        protocol->store((ulonglong)busy_Count);
        protocol->store((ulonglong)format_Count);
        protocol->store((ulonglong)reset_Count);
        protocol->write();
    }

    mysql_mutex_unlock(&global_trace_cache->trace_lock);
    
    my_eof(thd);
    DBUG_VOID_RETURN;
}

void proxy_rule_full_status(THD* thd)
{
    List<Item> field_list;
    Protocol *protocol= thd->protocol;
    MEM_ROOT *mem_root= thd->mem_root;
    
    DBUG_ENTER("proxy_rule_full_status");

    field_list.push_back(new (mem_root)
                         Item_int(thd, "rule_id", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_int(thd, "active", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "username", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "schemaname", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "client_addr", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "proxy_addr", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_int(thd, "proxy_port", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "digest", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "match_digest", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "match_pattern", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_int(thd, "negate_match_pattern", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "re_modifiers", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "replace_pattern", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "destination", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_int(thd, "timeout", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "error_msg", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_int(thd, "log", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "comment", USERNAME_CHAR_LENGTH), mem_root);
    
    if (protocol->send_result_set_metadata(&field_list,
                                           Protocol::SEND_NUM_ROWS |
                                           Protocol::SEND_EOF))
        DBUG_VOID_RETURN;
    
    int id=0;
    mysql_mutex_lock(&global_proxy_config.config_lock);
    for (std::vector<QP_rule_t *>::iterator it=global_proxy_config.rules.begin(); 
        it!=global_proxy_config.rules.end(); ++it) 
    {
        QP_rule_t* qr1=*it;
        protocol->prepare_for_resend();
        protocol->store((ulonglong)qr1->rule_id);
        protocol->store((ulonglong)qr1->active);
        protocol->store(qr1->username, system_charset_info);
        protocol->store(qr1->schemaname, system_charset_info);
        protocol->store(qr1->client_addr, system_charset_info);
        protocol->store(qr1->proxy_addr, system_charset_info);
        protocol->store((ulonglong)qr1->proxy_port);
        protocol->store(qr1->digest_str, system_charset_info);
        protocol->store(qr1->match_digest, system_charset_info);
        protocol->store(qr1->match_pattern, system_charset_info);
        protocol->store((ulonglong)qr1->negate_match_pattern);
        protocol->store(qr1->re_modifiers_str, system_charset_info);
        protocol->store(qr1->replace_pattern, system_charset_info);
        protocol->store(qr1->destination_hostgroup, system_charset_info);
        protocol->store((ulonglong)qr1->timeout);
        protocol->store(qr1->error_msg, system_charset_info);
        protocol->store((ulonglong)qr1->log);
        protocol->write();
    }

    mysql_mutex_unlock(&global_proxy_config.config_lock);
   
    my_eof(thd);
    DBUG_VOID_RETURN;
}

void proxy_trace_full_status(THD* thd)
{
    List<Item> field_list;
    Protocol *protocol= thd->protocol;
    MEM_ROOT *mem_root= thd->mem_root;
    
    DBUG_ENTER("proxy_trace_status");

    field_list.push_back(new (mem_root)
                         Item_int(thd, "Id", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Array_Index", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Thread_ID", 0, MY_INT64_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Queue_State", USERNAME_CHAR_LENGTH), mem_root);
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Enqueue_Index", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Dequeue_Index", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Digest_Sum", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    
    if (protocol->send_result_set_metadata(&field_list,
                                           Protocol::SEND_NUM_ROWS |
                                           Protocol::SEND_EOF))
        DBUG_VOID_RETURN;
    
    int id=0;
    mysql_mutex_lock(&global_trace_cache->trace_lock);
    if (trace_digest_inited)
    {
        for (int i = 0; i < global_trace_cache->trace_cache_size; ++i)
        {
            trace_queue_t* queue = &global_trace_cache->trace_queue[i];
            if (queue->trace_state == TRACE_STATE_EMPTY)
                break;
            if (queue->trace_state == TRACE_STATE_RESET)
                continue;
            protocol->prepare_for_resend();
            protocol->store((ulonglong)++id);
            protocol->store((ulonglong)i);
            protocol->store((ulonglong)queue->thread_id);
            if (queue->trace_state == TRACE_STATE_BUSY)
                protocol->store("Busy", system_charset_info);
            else if (queue->trace_state == TRACE_STATE_FORMAT)
                protocol->store("Formating", system_charset_info);
            protocol->store((ulonglong)queue->enqueue_index);
            protocol->store((ulonglong)queue->dequeue_index);
            protocol->store((ulonglong)queue->count);
            protocol->write();
        }
    }

    mysql_mutex_unlock(&global_trace_cache->trace_lock);
    
    my_eof(thd);
    DBUG_VOID_RETURN;
}

int proxy_config_trace_sql_create(THD* thd)
{
    if (proxy_trace_cache_init())
        return false;
    if (proxy_sql_trace_init())
        return false;
    my_ok(thd);
    return false;
}

int proxy_config_trace_create(THD* thd)
{
    if (proxy_trace_cache_init())
        return false;

    my_ok(thd);
    return false;
}

int proxy_config_trace_sql_close(THD* thd)
{
    if (proxy_sql_trace)
    {
        char err[1024];
        sprintf(err, "please set variable proxy_sql_trace to OFF first");
        my_error(ER_CONFIG_ERROR, MYF(0), err);
        return false;
    }

    if (!proxy_sql_trace_check_empty())
    {
        char err[1024];
        sprintf(err, "please wait the sql trace cache is empty");
        my_error(ER_CONFIG_ERROR, MYF(0), err);
        return false;
    }

    proxy_sql_trace_deinit();
    my_ok(thd);
    return false;
}

int proxy_config_trace_close(THD* thd)
{
    if (proxy_sql_trace || proxy_digest_trace)
    {
        char err[1024];
        sprintf(err, "please set variable proxy_sql_trace and proxy_digest_trace to OFF first");
        my_error(ER_CONFIG_ERROR, MYF(0), err);
        return false;
    }

    if (!proxy_trace_digest_check_empty())
    {
        char err[1024];
        sprintf(err, "please wait the digest trace cache is empty");
        my_error(ER_CONFIG_ERROR, MYF(0), err);
        return false;
    }

    if (!proxy_sql_trace_check_empty())
    {
        char err[1024];
        sprintf(err, "please wait the sql trace cache is empty");
        my_error(ER_CONFIG_ERROR, MYF(0), err);
        return false;
    }

    proxy_sql_trace_deinit();
    proxy_trace_cache_deinit();
    my_ok(thd);
    return false;
}

int proxy_config_write(THD* thd)
{
    str_t config_str;
    FILE* outfile;
    char err[512];
    proxy_servers_t* servers;
    proxy_server_t* server;
    proxy_router_t* router;
    char* path;

    if (proxy_skip_sync_config_cluster(thd))
    {
        my_ok(thd);
        return false;
    }

    if (thd->lex->ident.length > 0)
    {
        outfile = fopen(thd->lex->ident.str, "w+");
        path = thd->lex->ident.str;
    }
    else
    {
        path = (char*)my_defaults_file;
        outfile = fopen(my_defaults_file, "w+");
    }

    if (strlen(path) > 256)
    {
        sprintf(err, "config outfile path is too long");
        my_error(ER_CONFIG_ERROR, MYF(0), err);
        return true;
    }

    if (outfile == NULL)
    {
        sprintf(err, "Config outfile '%s' is invalid", path);
        my_error(ER_CONFIG_ERROR, MYF(0), err);
        return true;
    }

    str_init(&config_str);
    str_append(&config_str, "[arkproxy]\n");
    write_status_array_for_proxy(thd, enumerate_sys_vars(thd, true, OPT_GLOBAL), &config_str);
    write_out_var_exceptions("core-file", &config_str);

    mysql_mutex_lock(&global_proxy_config.config_lock);
    servers = LIST_GET_FIRST(global_proxy_config.server_lst);
    while (servers)
    {
        str_append(&config_str, "\n");
        str_append(&config_str, "[");
        str_append(&config_str, servers->server_name);
        str_append(&config_str, "]\n");
        sprintf(err, "%-40s", "proxy_type");
        str_append(&config_str, err);
        str_append(&config_str, " = ");
        str_append(&config_str, "server");
        str_append(&config_str, "\n");
        sprintf(err, "%-40s", "backend_host");
        str_append(&config_str, err);
        str_append(&config_str, " = ");
        str_append(&config_str, servers->backend_host);
        str_append(&config_str, "\n");
        sprintf(err, "%-40s", "backend_port");
        str_append(&config_str, err);
        str_append(&config_str, " = ");
        sprintf(err, "%ld", servers->backend_port);
        str_append(&config_str, err);
        str_append(&config_str, "\n");
        sprintf(err, "%-40s", "max_slave_lag");
        str_append(&config_str, err);
        str_append(&config_str, " = ");
        sprintf(err, "%ld", servers->max_slave_lag);
        str_append(&config_str, err);
        str_append(&config_str, "\n");
        sprintf(err, "%-40s", "server_status");
        str_append(&config_str, err);
        str_append(&config_str, " = ");
        sprintf(err, "%s", servers->server_status == SERVER_STATUS_ONLINE ? "ONLINE" : "OFFLINE");
        str_append(&config_str, err);
        str_append(&config_str, "\n");
        sprintf(err, "%-40s", "weight");
        str_append(&config_str, err);
        str_append(&config_str, " = ");
        sprintf(err, "%ld", servers->weight);
        str_append(&config_str, err);
        str_append(&config_str, "\n");
        if (servers->comments)
        {
            sprintf(err, "%-40s", "config_comment");
            str_append(&config_str, err);
            str_append(&config_str, " = ");
            str_append(&config_str, servers->comments);
            str_append(&config_str, "\n");
        }

        servers = LIST_GET_NEXT(link, servers);
    }

    router = LIST_GET_FIRST(global_proxy_config.router_lst);
    while (router)
    {
        str_append(&config_str, "\n");
        str_append(&config_str, "[");
        str_append(&config_str, router->router_name);
        str_append(&config_str, "]\n");
        sprintf(err, "%-40s", "proxy_type");
        str_append(&config_str, err);
        str_append(&config_str, " = ");
        str_append(&config_str, "router");
        str_append(&config_str, "\n");
        sprintf(err, "%-40s", "router_type");
        str_append(&config_str, err);
        str_append(&config_str, " = ");
        if (router->router_type == ROUTER_TYPE_RO)
            str_append(&config_str, "readonly");
        else 
            str_append(&config_str, "readwrite");
        str_append(&config_str, "\n");
        sprintf(err, "%-40s", "router_servers");
        str_append(&config_str, err);
        str_append(&config_str, " = ");
        if (router->router_type == ROUTER_TYPE_RO)
        {
            int first=true;
            server = LIST_GET_FIRST(global_proxy_config.ro_server_lst);
            while (server)
            {
                if (server->route->router_type == ROUTER_TYPE_RO)
                {
                    if (!first)
                        str_append(&config_str, ",");
                    str_append(&config_str, server->server->server_name);
                    first = false;
                }

                server = LIST_GET_NEXT(link, server);
            }

        }
        else if (router->router_type == ROUTER_TYPE_RW)
        {
            int first=true;
            server = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
            while (server)
            {
                if (server->route->router_type == ROUTER_TYPE_RW)
                {
                    if (!first)
                        str_append(&config_str, ",");
                    str_append(&config_str, server->server->server_name);
                    first = false;
                }

                server = LIST_GET_NEXT(link, server);
            }
        }

        str_append(&config_str, "\n");
        if (router->comments)
        {
            sprintf(err, "%-40s", "config_comment");
            str_append(&config_str, err);
            str_append(&config_str, " = ");
            str_append(&config_str, router->comments);
            str_append(&config_str, "\n");
        }

        router = LIST_GET_NEXT(link, router);
    }

    mysql_mutex_unlock(&global_proxy_config.config_lock);

    fwrite(str_get(&config_str), str_get_len(&config_str), 1, outfile);
    fclose(outfile);

    if (proxy_sync_config_cluster(thd, (char*)"config write outfile"))
        return true;

    my_ok(thd);
    return false;
}

int proxy_set_threads_reconfig(THD* thd)
{
    switch (thd->lex->sub_command)
    {
    case CFG_CMD_RELOAD:
        proxy_set_threads_reload(thd);
        break;
    case CFG_CMD_OFFLINE_SERVER:
    case CFG_CMD_ONLINE_SERVER:
    case CFG_CMD_USER_SET:
        proxy_set_status_server(thd);
        break;
    case CFG_ADD_SERVER:
        proxy_config_add_server(thd);
        break;
    case CFG_ADD_ROUTE:
        proxy_config_add_route(thd);
        break;
    case CFG_DELETE_ROUTE:
        proxy_config_delete_route(thd);
        break;
    case CFG_ONOFFLINE_ROUTE:
        proxy_config_onoffline_route(thd);
        break;
    case CFG_DELETE_SERVER:
        proxy_config_delete_server(thd);
        break;
    case CFG_RULE_ADD:
        proxy_config_add_rule(thd);
        break;
    case CFG_RULE_DELETE:
        proxy_config_delete_rule(thd);
        break;
    case CFG_RULE_DELETE_ALL:
        /* all delete no need to check */
        proxy_config_delete_rule(thd);
        //my_ok(thd);
        break;
    case CFG_RULE_UPDATE:
        proxy_config_update_rule(thd);
        break;
    case CFG_CMD_FLUSH:
        proxy_config_flush(thd);
        break;
    case CFG_CMD_SHOW_CACHE_LIST:
        proxy_show_config_cache(thd);
        break;
    case CFG_CMD_SHOW_USER_CONFIG_LIST:
        proxy_show_user_config_list(thd);
        break;
    case CFG_CMD_DELETE_CONFIG:
        proxy_config_delete(thd);
        break;
    case SQLCOM_SHOW_CONN:
        mysqld_list_connections(thd, NullS, thd->lex->verbose);
        break;
    case SQLCOM_SHOW_THREADPOOL:
        mysqld_list_thread_pool(thd, NullS, thd->lex->verbose);
        break;
    case SQLCOM_SHOW_BACKEND_CONN:
        mysqld_list_backend_connections(thd, NullS, thd->lex->verbose);
        break;
    case SQLCOM_SHOW_PROXY_STATUS:
        mysqld_show_proxy_status(thd, NullS, thd->lex->verbose);
        break;
    case SQLCOM_SHOW_PROXY_CONN_STATUS:
        mysqld_show_proxy_conn_status(thd, NullS, thd->lex->verbose);
        break;
    case SQLCOM_SHOW_BACKEND_SERVERS:
        mysqld_list_backend_servers(thd, NullS, thd->lex->verbose);
        break;
    case SQLCOM_SHOW_BACKEND_ROUTES:
        mysqld_list_backend_routes(thd, NullS, thd->lex->verbose);
        break;
    case SQLCOM_SHOW_TRACE_STATUS:
        proxy_trace_status(thd);
        break;
    case SHELL_CMD_FULL_TRACE_STATUS:
        proxy_trace_full_status(thd);
        break;
    case SQLCOM_SHOW_RULE_STATUS:
        proxy_rule_full_status(thd);
        break;
    case CONFIG_HELP:
        proxy_help(thd);
        break;
    case CFG_CMD_WRITE:
        proxy_config_write(thd);
        break;
    case CFG_TRACE_CLOSE:
        proxy_config_trace_close(thd);
        break;
    case CFG_TRACE_SQL_CLOSE:
        proxy_config_trace_sql_close(thd);
        break;
    case CFG_TRACE_CREATE:
        proxy_config_trace_create(thd);
        break;
    case CFG_TRACE_SQL_CREATE:
        proxy_config_trace_sql_create(thd);
        break;
    default:
        break;
    }

    return false;
}

int proxy_local_set_variables(THD* thd)
{
    int error = sql_set_variables(thd, &thd->lex->var_list, false);
    if (error == 0)
        my_ok(thd);
    return false;
}

void proxy_show_variables(THD* thd)
{
    mysql_local_show_variables(thd, true);
}

void proxy_help(THD* thd)
{
    List<Item> field_list;
    Protocol *protocol= thd->protocol;
    MEM_ROOT *mem_root= thd->mem_root;
    
    DBUG_ENTER("proxy_help");
    field_list.push_back(new (mem_root)
                         Item_int(thd, "Id", 0, MY_INT32_NUM_DECIMAL_DIGITS),
                         mem_root);
    
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Command",
                                           NAME_CHAR_LEN),
                         mem_root);
    field_list.push_back(new (mem_root)
                         Item_empty_string(thd, "Description",
                                           USERNAME_CHAR_LENGTH),
                         mem_root);
    
    if (protocol->send_result_set_metadata(&field_list,
                                           Protocol::SEND_NUM_ROWS |
                                           Protocol::SEND_EOF))
        DBUG_VOID_RETURN;
    
    for (int i=0; i< sizeof(HELP_INFO); ++i)
    {
        help_t h = HELP_INFO[i];
        if (h.command == NULL)
            break;
        protocol->prepare_for_resend();
        protocol->store((ulonglong)i+1);
        protocol->store(h.command, system_charset_info);
        protocol->store(h.comment, system_charset_info);
        protocol->write();
    }

    my_eof(thd);
    DBUG_VOID_RETURN;
}

int proxy_shell_dispatch(THD* thd)
{
    switch (thd->lex->sql_command)
    {
    case SQLCOM_CONFIG:
        proxy_set_threads_reconfig(thd);
        break;
    case SQLCOM_SHOW_VARIABLES:
        proxy_show_variables(thd);
        break;
    case SQLCOM_SET_OPTION:
        proxy_local_set_variables(thd);
        break;
    default:
        my_ok(thd);
        break;
    }
    
    return false;
}
