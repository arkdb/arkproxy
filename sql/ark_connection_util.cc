#include "sql_class.h"
#include "sql_acl.h"
#include "ark_config.h"
#include <sys/times.h>
#include "sql_common.h"
#include "set_var.h"
#include "sql_audit.h"
#include "sql_connect.h"
#include <mysql.h>
// #include <thread>

#define VERSION_56 50600
#define VERSION_57 50700
#define VERSION_MARIADB 100000

extern int get_set_var_sql(THD* thd, set_var_base *var, format_cache_node_t* format_node);
extern THD *find_thread_by_id(longlong id, bool query_id);
MYSQL* get_backend_connection(THD* thd, backend_conn_t* conn);
bool close_backend_conn(backend_conn_t *conn);

ulong start_timer(void)
{
#if defined(__WIN__)
    return clock();
#else
    struct tms tms_tmp;
    return times(&tms_tmp);
#endif
}

int get_version(char version[])
{
    int rest = 0;
    char version_arr_1[10][100];
    memset(version_arr_1, 0, 10*100);
    char version_arr_2[10][100];
    memset(version_arr_2, 0, 10*100);
    split_chars(version_arr_1, version, "-");
    int cnt = split_chars(version_arr_2, version_arr_1[0], ".");
    if (cnt == 3)
        rest = atoi(version_arr_2[0])*10000 + atoi(version_arr_2[1])*100 + atoi(version_arr_2[2]);
    return rest;
}

#if __CONSISTEND_READ__
int proxy_update_consistend_cache(
    consistend_cache_t* consistend_cache, 
    int instance,
    char* dbname,
    char* tablename,
    longlong update_time
)
{
    proxy_consistend_read_t* proxy_consistend;
    
    proxy_consistend = LIST_GET_FIRST(consistend_cache->consistend_read_lst);
    while (proxy_consistend)
    {
        if (!strcasecmp(proxy_consistend->dbname, dbname) &&
            !strcasecmp(proxy_consistend->tablename, tablename))
        {
            proxy_consistend->timestamp = update_time;
            break;
        }

        proxy_consistend = LIST_GET_NEXT(link, proxy_consistend);
    }

    if (!proxy_consistend)
    {
        proxy_consistend = 
          (proxy_consistend_read_t*)my_malloc(sizeof(proxy_consistend_read_t), MY_ZEROFILL);
        strcpy(proxy_consistend->dbname, dbname);
        strcpy(proxy_consistend->tablename, tablename);
        proxy_consistend->timestamp= update_time;
        proxy_consistend->instance = instance;
        LIST_ADD_LAST(link, consistend_cache->consistend_read_lst, proxy_consistend);
    }

    return false;
}

int mysql_fetch_slave_consistend_read_table(backend_conn_t* conn)
{
    char set_format[1024];
    MYSQL_RES * source_res;
    MYSQL_ROW source_row;
    MYSQL* mysql;
    consistend_cache_t* consistend_cache;
    
    DBUG_ENTER("mysql_fetch_slave_consistend_read_table");
    
    mysql = (MYSQL*)conn->get_mysql();
    sprintf(set_format, "select * from arkproxy_consistend_read.update_timestamp");
    if (mysql_real_query(mysql, set_format, strlen(set_format)))
    {
        my_message(mysql_errno(mysql), mysql_error(mysql), MYF(0));
        DBUG_RETURN(true);
    }
    
    if ((source_res = mysql_store_result(mysql)) == NULL)
    {
        my_message(mysql_errno(mysql), mysql_error(mysql), MYF(0));
        DBUG_RETURN(true);
    }
    
    mysql_mutex_lock(&conn->consistend_cache->consistend_lock);
    //if (conn->consistend_cache == &conn->consistend_cache1)
    //    consistend_cache = &conn->consistend_cache2;
    //else
    consistend_cache = &conn->consistend_cache1;

    source_row = mysql_fetch_row(source_res);
    while (source_row)
    {
        proxy_update_consistend_cache(consistend_cache, (int)strtoll(source_row[0], NULL, 10),
            source_row[1], (char*)source_row[2], 
            (longlong)strtoll(source_row[3], NULL, 10));
        source_row = mysql_fetch_row(source_res);
    }

    mysql_mutex_unlock(&conn->consistend_cache->consistend_lock);
    
    mysql_free_result(source_res);
    DBUG_RETURN(false);
}
#endif

int mysql_fetch_server_status(backend_conn_t* conn)
{
    char set_format[1024];
    MYSQL_RES * source_res;
    MYSQL_ROW source_row;
    MYSQL* mysql;
    
    DBUG_ENTER("mysql_fetch_server_status");
    
    backend_conn_t cluster_fixed_conn;
    //memset(&cluster_fixed_conn, 0, sizeof(backend_conn_t));
    init_backend_conn_info(&cluster_fixed_conn, conn->server->backend_host, backend_user, 
        backend_passwd, conn->server->backend_port);

    mysql = get_backend_connection(NULL, &cluster_fixed_conn);
    if (!mysql)
    {
        current_thd->clear_error();
        conn->slave_lag = ULONG_MAX;
        DBUG_RETURN(false);
    }

    sprintf(set_format, "show slave status");
    if (mysql_real_query(mysql, set_format, strlen(set_format)))
    {
        my_message(mysql_errno(mysql), mysql_error(mysql), MYF(0));
        /*todo: print error to log */
        current_thd->clear_error();
        close_backend_connection(&cluster_fixed_conn);
        DBUG_RETURN(true);
    }
    
    if ((source_res = mysql_store_result(mysql)) == NULL)
    {
        my_message(mysql_errno(mysql), mysql_error(mysql), MYF(0));
        /*todo: print error to log */
        current_thd->clear_error();
        close_backend_connection(&cluster_fixed_conn);
        DBUG_RETURN(true);
    }
    
    source_row = mysql_fetch_row(source_res);
    if (source_row)
    {
        if (source_row[32] == NULL || !strcasecmp(source_row[32], "NULL"))
            conn->slave_lag = ULONG_MAX;
        else
            conn->slave_lag = strtoll(source_row[32], NULL, 10);
    }
    
    mysql_free_result(source_res);
    close_backend_connection(&cluster_fixed_conn);
    DBUG_RETURN(false);
}

static const char *sql_mode_names[]=
{
    "REAL_AS_FLOAT", "PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE",
    "ONLY_FULL_GROUP_BY", "NO_UNSIGNED_SUBTRACTION", "NO_DIR_IN_CREATE",
    "POSTGRESQL", "ORACLE", "MSSQL", "DB2", "MAXDB", "NO_KEY_OPTIONS",
    "NO_TABLE_OPTIONS", "NO_FIELD_OPTIONS", "MYSQL323", "MYSQL40", "ANSI",
    "NO_AUTO_VALUE_ON_ZERO", "NO_BACKSLASH_ESCAPES", "STRICT_TRANS_TABLES",
    "STRICT_ALL_TABLES", "NO_ZERO_IN_DATE", "NO_ZERO_DATE",
    "ALLOW_INVALID_DATES", "ERROR_FOR_DIVISION_BY_ZERO", "TRADITIONAL",
    "NO_AUTO_CREATE_USER", "HIGH_NOT_PRECEDENCE", "NO_ENGINE_SUBSTITUTION",
    "PAD_CHAR_TO_FULL_LENGTH",
    0
};

static const unsigned int sql_mode_values[]=
{
    MODE_REAL_AS_FLOAT,
    MODE_PIPES_AS_CONCAT,
    MODE_ANSI_QUOTES,
    MODE_IGNORE_SPACE,
    MODE_ONLY_FULL_GROUP_BY,
    MODE_NO_UNSIGNED_SUBTRACTION,
    MODE_NO_DIR_IN_CREATE,
    MODE_POSTGRESQL,
    MODE_ORACLE,
    MODE_MSSQL,
    MODE_DB2,
    MODE_MAXDB,
    MODE_NO_KEY_OPTIONS,
    MODE_NO_TABLE_OPTIONS,
    MODE_NO_FIELD_OPTIONS,
    MODE_MYSQL323,
    MODE_MYSQL40,
    MODE_ANSI,
    MODE_NO_AUTO_VALUE_ON_ZERO,
    MODE_NO_BACKSLASH_ESCAPES,
    MODE_STRICT_TRANS_TABLES,
    MODE_STRICT_ALL_TABLES,
    MODE_NO_ZERO_IN_DATE,
    MODE_NO_ZERO_DATE,
    MODE_INVALID_DATES,
    MODE_ERROR_FOR_DIVISION_BY_ZERO,
    MODE_TRADITIONAL,
    MODE_NO_AUTO_CREATE_USER,
    MODE_HIGH_NOT_PRECEDENCE,
    MODE_NO_ENGINE_SUBSTITUTION,
    MODE_PAD_CHAR_TO_FULL_LENGTH,
    0
};

int get_sql_mode_from_str(char* mode)
{
    unsigned int i = 0;
    while (sql_mode_names[i])
    {
        if (strcmp(mode, sql_mode_names[i])==0)
            return sql_mode_values[i];
        i++;
    }

    return 0;
}

int get_sql_mode(THD*  thd, char* sqlmode)
{
    char*   sql_mode;
    char*   strToken;
    int     ret;

    if (!strlen(sqlmode))
        return false;

    thd->variables.sql_mode = 0;
    while((strToken = strtok_r(sqlmode, ",", &sql_mode)) != NULL)
    {
        thd->variables.sql_mode |= get_sql_mode_from_str(strToken);
				sqlmode = NULL;
    }

err:
    return false;
}

int mysql_fetch_connection_variables(THD* thd, backend_conn_t* conn)
{
    char set_format[1024];
    MYSQL_RES * source_res;
    MYSQL_ROW source_row;
    MYSQL* mysql;
    
    DBUG_ENTER("mysql_fetch_wait_timeout");
    mysql = (MYSQL*)conn->get_mysql(false);
    sprintf(set_format, "show variables where Variable_name "
        "in('wait_timeout', 'autocommit', 'sql_mode');");
    if (mysql_real_query(mysql, set_format, strlen(set_format)))
    {
        my_message(mysql_errno(mysql), mysql_error(mysql), MYF(0));
        DBUG_RETURN(true);
    }
    
    if ((source_res = mysql_store_result(mysql)) == NULL)
    {
        my_message(mysql_errno(mysql), mysql_error(mysql), MYF(0));
        DBUG_RETURN(true);
    }
    
    source_row = mysql_fetch_row(source_res);
    while (source_row)
    {
        if (!strcasecmp(source_row[0], "wait_timeout"))
        {
            conn->wait_timeout = strtoll(source_row[1], NULL, 10);
        }
        else if (!strcasecmp(source_row[0], "autocommit"))
        {
            conn->autocommit= (strcasecmp(source_row[1], "ON") == 0) ? true : false;
        }
        else if (conn->thd && !strcasecmp(source_row[0], "sql_mode"))
        {
            get_sql_mode(thd, source_row[1]);
        }

        source_row = mysql_fetch_row(source_res);
    }
    
    mysql_free_result(source_res);
    DBUG_RETURN(false);
}

int arkproxy_copy_env(THD* thd, backend_conn_t* conn, char* varname)
{
    int i;
    backend_conn_t* write_conn;

    if (thd->write_conn_count > 0)
    {
        for (i = 0; i < thd->write_conn_count; i++)
        {
            write_conn = thd->write_conn[i];
            if (!strcasecmp(varname, "autocommit"))
                write_conn->autocommit = conn->autocommit;
//            if (!strcasecmp(varname, "wait_timeout"))
//                write_conn->wait_timeout = conn->wait_timeout;
        }
    }

    if (thd->read_conn_count > 0)
    {
        for (i = 0; i < thd->read_conn_count; i++)
        {
            write_conn = thd->read_conn[i];
            if (!strcasecmp(varname, "autocommit"))
                write_conn->autocommit = conn->autocommit;
//            if (!strcasecmp(varname, "wait_timeout"))
//                write_conn->wait_timeout = conn->wait_timeout;
        }
    }

    return false;
}

bool init_backend_connection(THD* thd, backend_conn_t* conn)
{
    ulong client_flag= CLIENT_REMEMBER_OPTIONS;
    uint net_timeout= 3600*24;
    uint connect_timeout= 5;
    bool reconnect= TRUE;
    MYSQL* mysql = (MYSQL*)conn->get_mysql(false);
    mysql = mysql_init(mysql);
    conn->set_mysql(mysql);

    mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, (char *) &connect_timeout);
    mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, (char *) &net_timeout);
    mysql_options(mysql, MYSQL_OPT_WRITE_TIMEOUT, (char *) &net_timeout);
    mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "utf8mb4");
    // mysql_options(mysql, MYSQL_SET_CHARSET_DIR, (char *) charsets_dir);
    mysql_options(mysql, MYSQL_OPT_RECONNECT, (bool*)&reconnect);
    
    if (mysql_real_connect(mysql, conn->host, conn->user,
           conn->passwd, NULL, conn->port, NULL, client_flag) == 0)
    {
        my_message(mysql_errno(mysql), mysql_error(mysql), MYF(0));
        close_backend_conn(conn);   
        return FALSE;
    }
    
    conn->inited = TRUE;
    conn->thd = thd;
    if (thd && mysql_fetch_connection_variables(thd, conn))
    {
        close_backend_conn(conn);
        return false;
    }

    conn->start_timer = start_timer();
    return TRUE;
}

MYSQL* get_backend_connection(THD* thd, backend_conn_t* conn)
{
reconnect:
    if (!conn->conn_inited())
    {
        if (conn->host[0] == '\0' || conn->port == 0 ||
            conn->user[0] == '\0' || conn->passwd[0] == '\0')
        {
            my_error(ER_INVALID_BACKEND_HOST_INFO, MYF(0));
            return NULL;
        }
        if (init_backend_connection(thd, conn) == FALSE)
            return NULL;
        else
            return (MYSQL*)conn->get_mysql();
    }
    else
    {
        if (start_timer() - conn->start_timer >
            (conn->wait_timeout * CLOCKS_PER_SEC - 10))
        {
            conn->inited = false;
            /* update the timer */
            conn->start_timer = start_timer();
            sql_print_information("backend connection closed (wait_timeout: %d), reconnect",
                                  conn->wait_timeout);
            close_backend_conn(conn);
            goto reconnect;
        }
        
        return (MYSQL*)conn->get_mysql();
    }
}

void close_backend_connection(backend_conn_t* conn)
{
    if (conn == NULL)
        return;
    
    if (conn->conn_inited())
    {
        close_backend_conn(conn);
#if __CONSISTEND_READ__
        if (conn->consistend_cache)
        {
            proxy_consistend_read_t* proxy_consistend;
            proxy_consistend_read_t* proxy_consistend_next;
            
            proxy_consistend = LIST_GET_FIRST(conn->consistend_cache->consistend_read_lst);
            while (proxy_consistend)
            {
                proxy_consistend_next = LIST_GET_NEXT(link, proxy_consistend);
                LIST_REMOVE(link, conn->consistend_cache->consistend_read_lst, proxy_consistend);
                my_free(proxy_consistend);
                proxy_consistend = proxy_consistend_next;
            }
            mysql_mutex_destroy(&conn->consistend_cache->consistend_lock);
            conn->consistend_cache = NULL;
        }
#endif
    }
}

void close_ark_conn(THD* thd)
{
    backend_conn_t* write_conn;
    int i;
    mysql_mutex_lock(&LOCK_thread_count); 
    int write_count = thd->write_conn_count;
    int read_count = thd->read_conn_count;
    thd->write_conn_count = 0;
    thd->read_conn_count = 0;
    mysql_mutex_unlock(&LOCK_thread_count);

    close_backend_connection(&thd->cluster_fixed_conn);
    if (write_count > 0)
    {
        for (i = 0; i < write_count; i++)
        {
            write_conn = thd->write_conn[i];
            write_conn->conn_inited();
            close_backend_connection(write_conn);
            my_free(write_conn);
        }
    }
    if (read_count > 0)
    {
        for (i = 0; i < read_count; i++)
        {
            write_conn = thd->read_conn[i];
            write_conn->conn_inited();
            close_backend_connection(write_conn);
            my_free(write_conn);
        }
    }
}

int cmp_backend_thread_id(THD* thd, unsigned long thread_id)
{
    backend_conn_t* write_conn;
    int i;

    if (thd->write_conn_count > 0)
    {
        for (i = 0; i < thd->write_conn_count; i++)
        {
            write_conn = thd->write_conn[i];
            MYSQL* mysql = (MYSQL*)write_conn->get_mysql();
            if (mysql && mysql->thread_id == thread_id)
                return true;
        }
    }
    if (thd->read_conn_count > 0)
    {
        for (i = 0; i < thd->read_conn_count; i++)
        {
            write_conn = thd->read_conn[i];
            MYSQL* mysql = (MYSQL*)write_conn->get_mysql();
            if (mysql && mysql->thread_id == thread_id)
                return true;
        }
    }
    return false;
}

uint
proxy_kill_one_thread(THD *thd, longlong id, killed_state kill_signal, killed_type type)
{
    THD *tmp;
    uint error= (type == KILL_TYPE_QUERY ? ER_NO_SUCH_QUERY : ER_NO_SUCH_THREAD);
    DBUG_ENTER("kill_one_thread");
    DBUG_PRINT("enter", ("id: %lld  signal: %u", id, (uint) kill_signal));
    
    if (id && (tmp= find_thread_by_id(id, type == KILL_TYPE_QUERY)))
    {
        /*
         If we're SUPER, we can KILL anything, including system-threads.
         No further checks.
         
         KILLer: thd->security_ctx->user could in theory be NULL while
         we're still in "unauthenticated" state. This is a theoretical
         case (the code suggests this could happen, so we play it safe).
         
         KILLee: tmp->security_ctx->user will be NULL for system threads.
         We need to check so Jane Random User doesn't crash the server
         when trying to kill a) system threads or b) unauthenticated users'
         threads (Bug#43748).
         
         If user of both killer and killee are non-NULL, proceed with
         slayage if both are string-equal.
         
         It's ok to also kill DELAYED threads with KILL_CONNECTION instead of
         KILL_SYSTEM_THREAD; The difference is that KILL_CONNECTION may be
         faster and do a harder kill than KILL_SYSTEM_THREAD;
         */
        
        if (((thd->security_ctx->backend_access & SUPER_ACL) ||
             thd->security_ctx->user_matches(tmp->security_ctx)) &&
            !wsrep_thd_is_BF(tmp, true))
        {
            tmp->awake(kill_signal);
            error=0;
        }
        else
            error= (type == KILL_TYPE_QUERY ? ER_KILL_QUERY_DENIED_ERROR :
                    ER_KILL_DENIED_ERROR);

        mysql_mutex_unlock(&tmp->LOCK_thd_data);
    }
    DBUG_PRINT("exit", ("%d", error));
    DBUG_RETURN(error);
}

uint proxy_kill_threads_for_user(THD *thd, LEX_USER *user,
                                 killed_state kill_signal, ha_rows *rows)
{
    THD *tmp;
    List<THD> threads_to_kill;
    DBUG_ENTER("kill_threads_for_user");
    
    *rows= 0;
    
    if (thd->is_fatal_error)                       // If we run out of memory
        DBUG_RETURN(ER_OUT_OF_RESOURCES);
    
    DBUG_PRINT("enter", ("user: %s  signal: %u", user->user.str,
                         (uint) kill_signal));
    
    mysql_mutex_lock(&LOCK_thread_count); // For unlink from list
    I_List_iterator<THD> it(threads);
    while ((tmp=it++))
    {
        if (!tmp->security_ctx->user)
            continue;
        /*
         Check that hostname (if given) and user name matches.
         
         host.str[0] == '%' means that host name was not given. See sql_yacc.yy
         */
        if (((user->host.str[0] == '%' && !user->host.str[1]) ||
             !strcmp(tmp->security_ctx->host_or_ip, user->host.str)) &&
            !strcmp(tmp->security_ctx->user, user->user.str))
        {
            if (!(thd->security_ctx->master_access & SUPER_ACL) &&
                !thd->security_ctx->user_matches(tmp->security_ctx))
            {
                mysql_mutex_unlock(&LOCK_thread_count);
                DBUG_RETURN(ER_KILL_DENIED_ERROR);
            }
            if (!threads_to_kill.push_back(tmp, thd->mem_root))
                mysql_mutex_lock(&tmp->LOCK_thd_data); // Lock from delete
        }
    }
    mysql_mutex_unlock(&LOCK_thread_count);
    if (!threads_to_kill.is_empty())
    {
        List_iterator_fast<THD> it2(threads_to_kill);
        THD *next_ptr;
        THD *ptr= it2++;
        do
        {
            ptr->awake(kill_signal);
            /*
             Careful here: The list nodes are allocated on the memroots of the
             THDs to be awakened.
             But those THDs may be terminated and deleted as soon as we release
             LOCK_thd_data, which will make the list nodes invalid.
             Since the operation "it++" dereferences the "next" pointer of the
             previous list node, we need to do this while holding LOCK_thd_data.
             */
            next_ptr= it2++;
            mysql_mutex_unlock(&ptr->LOCK_thd_data);
            (*rows)++;
        } while ((ptr= next_ptr));
    }
    DBUG_RETURN(0);
}

void proxy_sql_kill_user(THD *thd, LEX_USER *user, killed_state state)
{
    uint error;
    ha_rows rows;
    if (!(error= proxy_kill_threads_for_user(thd, user, state, &rows)))
        my_ok(thd, rows);
    else
    {
        /*
         This is probably ER_OUT_OF_RESOURCES, but in the future we may
         want to write the name of the user we tried to kill
         */
        my_error(error, MYF(0), user->host.str, user->user.str);
    }
}

void
proxy_sql_kill(THD *thd, longlong id, killed_state state, killed_type type)
{
    uint error;
    if (!(error= proxy_kill_one_thread(thd, id, state, type)))
    {
        if ((!thd->killed))
            my_ok(thd);
        else
            my_error(killed_errno(thd->killed), MYF(0), id);
    }
    else
        my_error(error, MYF(0), id);
}

void close_thd_env(THD* thd)
{
    close_ark_conn(thd);
    backend_env_t* env = LIST_GET_FIRST(thd->env_list);
    while (env)
    {
        backend_env_t* env_next = LIST_GET_NEXT(link, env);
        LIST_REMOVE(link, thd->env_list, env);
        str_truncate_0(&env->str);
        str_deinit(&env->str);
        str_truncate_0(&env->name);
        str_deinit(&env->name);
        my_free(env);
        env = env_next;
    }
}

void init_backend_conn_info(backend_conn_t* conn, char* host, char* user, char* passwd, uint port)
{
    strcpy(conn->host,host);
    strcpy(conn->user,user);
    strcpy(conn->passwd,passwd);
    conn->port = port;
    conn->inited = false;
    conn->start_timer = 0;
    conn->env_version = 0;
    close_backend_conn(conn);
    conn->consistend_cache = NULL;
}

void init_cluster_fixed_conn(THD* thd)
{
    proxy_servers_t* server_node;
    backend_conn_t* conn = &thd->cluster_fixed_conn;
    memset(&thd->cluster_fixed_conn, 0, sizeof(backend_conn_t));
    //conn = (backend_conn_t*)my_malloc(sizeof(backend_conn_t), MY_ZEROFILL);
    proxy_server_t* server = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
    while (server)
    {
        if (server->server->server_status == SERVER_STATUS_ONLINE)
        {
            server_node = server->server;
            init_backend_conn_info(conn, server_node->backend_host, backend_user,
                                   backend_passwd, server_node->backend_port);
        }

        server = LIST_GET_NEXT(link, server);
    }

    if (!server)
    {
        conn->inited = false;
    }

    //thd->cluster_fixed_conn = *conn;
}

void handle_send_env_error(backend_conn_t* conn)
{
    int err_no = mysql_errno((MYSQL*)conn->get_mysql());
    /* Lost connection to server during query and 
    MySQL server has gone away */
    if (err_no == 2013 || err_no == 2006) 
        conn->inited = false;    //等待重连
    else
    {
        /* TODO: how to process? retry? */
        conn->inited = conn->inited;
    }
}

backend_env_t* get_ark_env_from_list(THD* thd, enum enum_sql_command type, const char* name)
{
    backend_env_t* env = LIST_GET_FIRST(thd->env_list);
    while (env)
    {
        if (env->type == type && strcasecmp(env->name.str, name) == 0)
            return env;
        env = LIST_GET_NEXT(link, env);
    }
    return NULL;
}

enum enum_sql_command get_env_type(THD* thd)
{
    switch (thd->get_command()) {
        case COM_INIT_DB:
            return SQLCOM_CHANGE_DB;
            
        case COM_QUERY:
            if (thd->lex->sql_command == SQLCOM_SET_OPTION)
                return SQLCOM_SET_OPTION;
            if (thd->lex->sql_command == SQLCOM_CHANGE_DB)
                return SQLCOM_CHANGE_DB;
                // return packet->sub_command;
        default:
            return SQLCOM_END;
    }
}

backend_env_t* get_ark_env(THD* thd, const char* name)
{
    enum enum_sql_command type = get_env_type(thd);

    backend_env_t* env = get_ark_env_from_list(thd, type, name);
    if (env == NULL)
    {
        env = (backend_env_t*)my_malloc(sizeof(backend_env_t), MY_ZEROFILL);
        str_init(&env->str);
        str_init(&env->name);
        env->type = type;
        env->env_version = 0;
        LIST_ADD_LAST(link, thd->env_list, env);
    }
    else
    {
        LIST_REMOVE(link, thd->env_list, env);
        LIST_ADD_LAST(link, thd->env_list, env);
    }
    
    return env;
}

int is_env_option(enum_sql_command command)
{
    switch(command)
    {
    case SQLCOM_CHANGE_DB:
    case SQLCOM_SET_OPTION:
        return true;
    default:
        return false;
    }

    return false;
}

void set_env_info(THD* thd, backend_conn_t* send_conn,
                  char* str, uint str_len,
                  const char* name, uint name_len)
{
    backend_env_t* env = get_ark_env(thd, name);
    str_truncate_0(&env->str);
    str_truncate_0(&env->name);
    str_append_with_length(&env->name, name, name_len);
    str_append_with_length(&env->str, str, str_len);
    /* keep the newest version of current connection and thd */
    env->env_version = thd->env_version;
    if (send_conn)
        send_conn->env_version = env->env_version;
}

static const char *env_variables[]=
{
    "wait_timeout", "autocommit", "sql_mode", 
    0
};

int env_update_check(char* varname)
{
    int i = 0; 
    while (env_variables[i])
    {
        if (!strcasecmp(varname, env_variables[i]))
            return true;
        i ++;
    }

    return false;
}

void set_var_info(THD *thd, set_var_base *var, backend_conn_t* send_conn)
{
    format_cache_node_t* format_node = 
      (format_cache_node_t*)my_malloc(sizeof(format_cache_node_t), MY_ZEROFILL);
    format_node->sql_statements = (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
    format_node->format_sql= (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
    str_init(format_node->format_sql);
    str_init(format_node->sql_statements);

    do
    {
        if (!strcasecmp(var->set_var_type, "set_var"))
        {
            set_var* s_var = dynamic_cast <set_var*> (var);
            str_truncate_0(format_node->format_sql);
            str_truncate_0(format_node->sql_statements);
            get_set_var_sql(thd, var, format_node);

            set_env_info(thd, send_conn, 
                format_node->format_sql->str, 
                format_node->format_sql->cur_len,
                (char*)s_var->var->name.str, s_var->var->name.length);

            if (env_update_check((char*)s_var->var->name.str))
            {
                mysql_fetch_connection_variables(thd, send_conn);
                /* when set the env, then sync this new value to other conns */
                arkproxy_copy_env(thd, send_conn, (char*)s_var->var->name.str);
            }
        }
        else if (!strcasecmp(var->set_var_type, "set_user_var"))
        {
            set_var_user* s_var = dynamic_cast <set_var_user*> (var);
            str_truncate_0(format_node->format_sql);
            str_truncate_0(format_node->sql_statements);
            get_set_var_sql(thd, var, format_node);
            if (format_node->format_sql->cur_len > 0)
            {
                set_env_info(thd, send_conn, 
                    format_node->format_sql->str, 
                    format_node->format_sql->cur_len,
                    s_var->get_var_user()->name.str,
                    s_var->get_var_user()->name.length);
            }
        }
    } while ((var=thd->lex->var_list.pop()));

    deinit_format_cache_node(format_node, true);
}

void set_ark_env(THD* thd, backend_conn_t* send_conn)
{
    enum_sql_command type = get_env_type(thd);
    
    /* if when current sql executed, then set this conn env to newest(thd->env_version)
     * because the current sql is the real sql to execute, the before env sql is added 
     * by arkproxy, so it can not goto the below code to set: send_conn->env_version*/
    send_conn->env_version = thd->env_version;

    if (!is_env_option(type))
        return;

    thd->env_version++;

    if (type == SQLCOM_CHANGE_DB)
    {
        set_env_info(thd, send_conn, thd->db, thd->db_length, ENV_USE_DB, strlen(ENV_USE_DB));
    }
    else if (type == SQLCOM_SET_OPTION)
    {
        set_var_base *var = thd->lex->var_list.pop();
        if (var && !strcasecmp(var->set_var_type, "set_var_collation"))
        {
            set_env_info(thd, send_conn, (char*)thd->query(), thd->query_length(),
                         ENV_SET_NAMES, strlen(ENV_SET_NAMES));
        }
        else if (var != NULL)
        {
            set_var_info(thd, var, send_conn);
        }
        else if (var == NULL)
        {
            set_env_info(thd, send_conn, thd->query(), thd->query_length(),
                         thd->query(), thd->query_length());
        }
    }
}

void set_ark_env_for_connect(THD* thd)
{
    net_packet_t packet;
    if (thd->env_version >= 1)
        return;

    thd->env_version = 1;
    if (thd->db != NULL)
    {
        packet.command = COM_INIT_DB;
        backend_env_t* env = get_ark_env(thd, ENV_USE_DB);
        env->env_version = 1;
        str_truncate_0(&env->str);
        env->type = SQLCOM_CHANGE_DB;
        str_append_with_length(&env->str, thd->db, thd->db_length);
        str_append_with_length(&env->name, ENV_USE_DB, strlen(ENV_USE_DB));
    }
    char c_query[100];
    memset(c_query, 0 ,100);
    sprintf(c_query, "set names %s", thd->charset()->csname);
    
    packet.command = COM_QUERY;
    packet.sub_command = SQLCOM_SET_OPTION;
    backend_env_t* env = get_ark_env(thd, ENV_SET_NAMES);
    env->env_version = 1;
    str_truncate_0(&env->str);
    env->type = SQLCOM_SET_OPTION;
    if (strcasecmp("utf8", thd->charset()->csname) == 0)
        str_append_with_length(&env->str, "set names utf8mb4", 18);
    else
        str_append_with_length(&env->str, c_query, 10 + strlen(thd->charset()->csname));
    str_append_with_length(&env->name, ENV_SET_NAMES, strlen(ENV_SET_NAMES));
}

backend_conn_t* get_cluster_fixed_conn(THD* thd)
{
    if (thd->config_version < global_proxy_config.config_version)
        init_cluster_fixed_conn(thd);
    
    if (thd->cluster_fixed_conn.inited == false)
    {
        MYSQL *mysql = get_backend_connection(thd, &thd->cluster_fixed_conn);
        if (mysql)
            thd->cluster_fixed_conn.set_mysql(mysql);
        else
            return NULL;
    }

    return &thd->cluster_fixed_conn;
}

bool get_cluster_auth_passwd(THD* thd, char* user, char* host, char* ip, char* password)
{
    bool check_rest = false;
    MYSQL_RES* source_res= 0;
    MYSQL_ROW  source_row;

    backend_conn_t* conn = get_cluster_fixed_conn(thd);
    if (conn == NULL)
        return check_rest;
    MYSQL* mysql = (MYSQL*)conn->get_mysql();
    char sql[1024] = "";
    int version = 0;

    sprintf(sql, "show columns from mysql.user where Field='password';");
    if (!mysql_real_query(mysql, sql, strlen(sql)))
    {
        if ((source_res = mysql_store_result(mysql)) != NULL)
        {
            version = source_res->row_count;
            //source_row = mysql_fetch_row(source_res);
            //char version_tmp[30] = "";
            //strcpy(version_tmp, source_row[0]);
            //version = get_version(version_tmp);
            mysql_free_result(source_res);
        }
    }

    if (version)
    {
        sprintf(sql, "select case when password='' then "
            "authentication_string else password  end as Password, "
            "Host, Super_priv from mysql.user where user = '%s'", user);
    }
    else
    {
        sprintf(sql, "select authentication_string as Password, "
            "Host, Super_priv from mysql.user where user = '%s'", user);
    }

    if (!mysql_real_query(mysql, sql, strlen(sql)))
    {
        if ((source_res = mysql_store_result(mysql)) != NULL)
        {
            source_row = mysql_fetch_row(source_res);
            while (source_row)
            {
                const char* temp_host = source_row[1];
                strmake(thd->security_ctx->priv_host, temp_host, MAX_HOSTNAME - 1);
                strmake(thd->security_ctx->priv_user, user, MAX_HOSTNAME - 1);
                acl_host_and_ip host_and_ip;
                update_hostname(&host_and_ip, temp_host);
                if (compare_hostname(&host_and_ip, host, ip))
                {
                    strcpy(password, source_row[0]);
                    check_rest = true;
                    const char* sup_priv = source_row[2];
                    if (strcasecmp((char*)sup_priv, "Y") == 0)
                        thd->security_ctx->backend_access = 
                          thd->security_ctx->backend_access | SUPER_ACL;
                    break;
                }
                source_row = mysql_fetch_row(source_res);
            }
            mysql_free_result(source_res);
        }
    }

    close_backend_connection(conn);
    return check_rest;
}

void proxy_change_user(THD* thd, NET *net, char* packet, uint packet_length)
{
    int auth_rc;
    status_var_increment(thd->status_var.com_other);
    
    thd->change_user();
    thd->clear_error();                         // if errors from rollback
    
    /* acl_authenticate() takes the data from net->read_pos */
    net->read_pos= (uchar*)packet;
    
    uint save_db_length= thd->db_length;
    char *save_db= thd->db;
    USER_CONN *save_user_connect= thd->user_connect;
    Security_context save_security_ctx= *thd->security_ctx;
    CHARSET_INFO *save_character_set_client=
    thd->variables.character_set_client;
    CHARSET_INFO *save_collation_connection=
    thd->variables.collation_connection;
    CHARSET_INFO *save_character_set_results=
    thd->variables.character_set_results;
    
    /* Ensure we don't free security_ctx->user in case we have to revert */
    thd->security_ctx->user= 0;
    thd->user_connect= 0;
    
    /*
     to limit COM_CHANGE_USER ability to brute-force passwords,
     we only allow three unsuccessful COM_CHANGE_USER per connection.
     */
    if (thd->failed_com_change_user >= 3)
    {
        my_message(ER_UNKNOWN_COM_ERROR, ER_THD(thd,ER_UNKNOWN_COM_ERROR),
                   MYF(0));
        auth_rc= 1;
    }
    else
        auth_rc= acl_authenticate(thd, packet_length);
    
    mysql_audit_notify_connection_change_user(thd);
    if (auth_rc)
    {
        /* Free user if allocated by acl_authenticate */
        my_free(thd->security_ctx->user);
        *thd->security_ctx= save_security_ctx;
        if (thd->user_connect)
            decrease_user_connections(thd->user_connect);
        thd->user_connect= save_user_connect;
        thd->reset_db(save_db, save_db_length);
        thd->update_charset(save_character_set_client, save_collation_connection,
                            save_character_set_results);
        thd->failed_com_change_user++;
        my_sleep(1000000);
    }
    else
    {
#ifndef NO_EMBEDDED_ACCESS_CHECKS
        /* we've authenticated new user */
        if (save_user_connect)
            decrease_user_connections(save_user_connect);
#endif /* NO_EMBEDDED_ACCESS_CHECKS */
        my_free(save_db);
        my_free(save_security_ctx.user);
    }
}

int proxy_connection_can_route_read(backend_conn_t* conn)
{
    int slave_lag = conn->slave_lag != 0 && conn->slave_lag != ULONG_MAX
    && conn->server->max_slave_lag < conn->slave_lag;
    int can_route = !slave_lag && conn->conn_inited() &&
    conn->server->server_status == SERVER_STATUS_ONLINE;
    return can_route;
}

backend_conn_t* get_first_request_best_conn(THD* thd)
{
    int total_weight = 0;
    backend_conn_t* best = NULL;
    for (int  i=0; i<thd->read_conn_count; ++i)
    {
        backend_conn_t* conn = thd->read_conn[i];
        int can_route = proxy_connection_can_route_read(conn);
        conn->server->current_weight += can_route ? conn->server->weight : 0;  //update server's current_weight
        total_weight += can_route ? conn->server->weight : 0;
        if (can_route && conn->autocommit && thd->first_request
            && conn->server == thd->first_request_best_server)
        {
            best = conn;
        }
    }
    if (best)
    {
        mysql_mutex_lock(&global_proxy_config.current_weight_lock);
        thd->first_request_best_server->current_weight -= total_weight;
        mysql_mutex_unlock(&global_proxy_config.current_weight_lock);
    }
    return best;
}

backend_conn_t* load_balance_for_read(THD* thd)
{
    int total_weight = 0;
    backend_conn_t* best = NULL;
    if (thd->first_request)
    {
        thd->first_request = false;
        best = get_first_request_best_conn(thd);
        if (best)
            return best;
    }
    for (int i=0; i< thd->read_conn_count; ++i)
    {
        backend_conn_t* conn = thd->read_conn[i];
        int can_route  = proxy_connection_can_route_read(conn);
        conn->current_weight += can_route ? conn->server->weight : 0;
        total_weight += can_route ? conn->server->weight : 0;

        /* if the conn is not autocommit, then this read node is not routed */
        if (can_route && conn->autocommit && 
            (best == NULL || conn->current_weight > best->current_weight))
        {
            best = conn;
        }
    }

    if (best)
        best->current_weight -= total_weight;
    return best;
}

backend_conn_t* get_read_connection(THD* thd)
{
    if (thd->read_conn_count == 0)
        return NULL;
    return load_balance_for_read(thd);
}

int proxy_connection_can_route_write(
    backend_conn_t* conn 
)
{
    if (conn->conn_inited() && conn->server->server_status == SERVER_STATUS_ONLINE)
        return true;

    return false;
}

/* 
 * if the proxy_multi_write_mode is on, and command is prepare, 
 * then this command can not been balanced route, need to route
 * one node always
 * */
backend_conn_t* proxy_route_to_first_write(THD* thd)
{
    for (int i=0; i< thd->write_conn_count; ++i)
    {
        backend_conn_t* conn = thd->write_conn[i];
        // int slave_lag = conn->slave_lag != 0 && conn->server->max_slave_lag < conn->slave_lag;
        int can_route = proxy_connection_can_route_write(conn);
        if (can_route)
            return conn;
    }

    return NULL;
}

/* 
 * if the proxy_multi_write_mode is on, then write is load balance, 
 * if it is off, then there is only one write online node
 * */
backend_conn_t* load_balance_for_write(THD* thd)
{
    int total_weight = 0;
    backend_conn_t* best = NULL;
    
    for (int i=0; i< thd->write_conn_count; ++i)
    {
        backend_conn_t* conn = thd->write_conn[i];
        // int slave_lag = conn->slave_lag != 0 && conn->server->max_slave_lag < conn->slave_lag;
        int can_route = proxy_connection_can_route_write(conn);
        conn->current_weight += can_route ? conn->server->weight : 0;
        total_weight += can_route ? conn->server->weight : 0;
        if (can_route && (best == NULL || conn->current_weight > best->current_weight))
            best = conn;
    }

    if (best)
        best->current_weight -= total_weight;
    return best;
}

backend_conn_t* get_write_connection(THD* thd)
{
    my_hrtime_t ctime;
    
    ctime = my_hrtime();
    if (thd->write_conn_count == 0)
        return NULL;
    return load_balance_for_write(thd);
    // return thd->write_conn[ctime.val % thd->write_conn_count];
}

backend_conn_t* 
add_write_connection(
    THD* thd, 
    proxy_servers_t* server, 
    char* user, 
    char* md5_hash, 
    char* host, 
    uint port
)
{
    backend_conn_t* conn;

    conn = (backend_conn_t*)my_malloc(sizeof(backend_conn_t), MY_ZEROFILL);
    //PRINT_MALLOC("add write connection", backend_conn_t);
    thd->write_conn[thd->write_conn_count++] = conn;
    
    if (user == NULL)
        user = md5_hash;
    strcpy(conn->user, user);
    strcpy(conn->md5_hash, md5_hash);
    strcpy(conn->host, host);
    if (thd->db)
        strcpy(conn->db, thd->db);
    else
        strcpy(conn->db, "");
    conn->port = port;
    conn->server = server;
    conn->env_version = 0;
    conn->current_weight = 0;
    conn->consistend_cache = NULL;
    return conn;
}

backend_conn_t* 
add_read_connection(
    THD* thd, 
    proxy_servers_t* server, 
    char* user, 
    char* md5_hash, 
    char* host, 
    uint port
)
{
    backend_conn_t* conn;

    conn = (backend_conn_t*)my_malloc(sizeof(backend_conn_t), MY_ZEROFILL);
    //PRINT_MALLOC("add read connection", backend_conn_t);
    thd->read_conn[thd->read_conn_count++] = conn;
    
    if (user == NULL)
        user = md5_hash;
    strcpy(conn->user, user);
    strcpy(conn->md5_hash, md5_hash);
    strcpy(conn->host, host);
    if (thd->db)
        strcpy(conn->db, thd->db);
    else
        strcpy(conn->db, "");
    conn->port = port;
    conn->server = server;
    conn->env_version = 0;
    conn->current_weight = 0;
    conn->conn_async_inited = false;
    conn->conn_async_complete = false;

#if __CONSISTEND_READ__
    conn->consistend_cache = NULL;
    if (thd->proxy_enable_consistend_read)
    {
        conn->consistend_cache = &conn->consistend_cache1;
        mysql_mutex_init(NULL, &conn->consistend_cache->consistend_lock, MY_MUTEX_INIT_FAST);
        LIST_INIT(conn->consistend_cache->consistend_read_lst);
    }
#endif

    return conn;
}

bool close_backend_conn(backend_conn_t *conn) {
  conn->conn_inited();
  if (conn->get_mysql(false)) {
    mysql_close((MYSQL *)conn->get_mysql(false));
    conn->set_mysql(NULL);
  }
  conn->inited = false;
  conn->conn_async_complete = false;
  conn->conn_async_inited = false;
  if (conn->async_thd) {
    delete conn->async_thd;
    conn->async_thd = NULL;
  }
}

pthread_handler_t proxy_reconnect_server_thread(void* arg)
{
  backend_conn_t* conn;
  THD* thd;

  conn = (backend_conn_t*)arg;
  thd = conn->thd;
  proxy_reconnect_server(thd, conn);
  return NULL;
}

int proxy_reconnect_server(THD* thd, backend_conn_t* conn)
{
    proxy_servers_t* server;
    Security_context *sctx= thd->security_ctx;
    uint net_timeout= 3600*24;
    bool reconnect= TRUE;
    uint connect_timeout= 5;
    ulong client_flag= CLIENT_REMEMBER_OPTIONS | CLIENT_MULTI_STATEMENTS
                            | CLIENT_MULTI_RESULTS | CLIENT_PS_MULTI_RESULTS 
                            | CLIENT_MULTI_STATEMENTS;

    conn->inited = false;
    int err = false;
    if(conn->get_mysql(false)) {
      sql_print_information("connection is not closed, reset connection");
      MYSQL* old_mysql = (MYSQL *)conn->get_mysql(false);
      conn->set_mysql(NULL);
      mysql_close(old_mysql);
    }
    server = conn->server;
    MYSQL* mysql = mysql_init(NULL);
    conn->set_mysql(mysql);
    conn->conn_async_complete = false;
    mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, (char *) &connect_timeout);
    mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, (char *) &net_timeout);
    mysql_options(mysql, MYSQL_OPT_WRITE_TIMEOUT, (char *) &net_timeout);
    mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "utf8mb4");
    // mysql_options(mysql, MYSQL_SET_CHARSET_DIR, (char *) charsets_dir);
    mysql_options(mysql, MYSQL_OPT_RECONNECT, (bool*)&reconnect);

    if (current_thd == NULL && conn->async_thd) {
        set_current_thd(conn->async_thd);
    }
    char* db = NULL;
    if(thd->db && thd->db[0] != '\0') {
        db = thd->db;
    }
    if (!proxy_connect_backend_server(mysql, (const char *)server->backend_host,
                                      thd->main_security_ctx.ip, 
                                      (const char *)sctx->user, NULL, db,
                                      server->backend_port, thd->peer_port, client_flag,
                                      (const uchar *)thd->server_hash_stage1)) {
      sql_print_warning("connection failed, host: %s, port: %d, error: %s",
                        server->backend_host, server->backend_port, mysql_error(mysql));
      mysql_close(mysql);
      conn->set_mysql(NULL);
      err = true;
      goto end;
    }

    conn->env_version = 0;
    conn->thd = thd;
    conn->last_heartbeat = my_hrtime().val;
    conn->last_execute = conn->last_heartbeat;

    if (mysql_fetch_connection_variables(thd, conn))
    {
        err = true;
        goto end;
    }
    conn->inited = true;
end:
    conn->conn_async_complete = true;
    return err;
}

int mysql_sleep(THD* thd)
{
    struct timespec abstime;
    mysql_mutex_t sleep_lock;
    mysql_cond_t sleep_cond;

    mysql_mutex_init(NULL, &sleep_lock, MY_MUTEX_INIT_FAST);
    mysql_cond_init(NULL, &sleep_cond, 0);

    set_timespec_nsec(abstime, 10 * 1000000ULL);
    mysql_mutex_lock(&sleep_lock);
    mysql_cond_timedwait(&sleep_cond, &sleep_lock, &abstime);
    mysql_mutex_unlock(&sleep_lock);

    mysql_cond_destroy(&sleep_cond);
    mysql_mutex_destroy(&sleep_lock);

    return false;
}

int proxy_reconnect_servers(THD* thd)
{
    Security_context *sctx= thd->security_ctx;
    proxy_server_t* server_ptr; 
    proxy_servers_t* server; 
    int entered = false;
    backend_conn_t* conn;
    char* ip_or_host;
    int connection_count = 0;
    int normal_count = 0;

    ip_or_host = (char*)(sctx->ip ? sctx->ip : sctx->host_or_ip);

    /* if reload is running, then wait until reload done */
retry:
    if (global_proxy_config.setting)
    {
        mysql_sleep(thd);
        goto retry;
    }

    close_ark_conn(thd);

    mysql_mutex_lock(&global_proxy_config.config_lock);
    server_ptr = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
    while (server_ptr)
    {
        server = server_ptr->server;
        if (!entered)
            conn = add_write_connection(thd, server, sctx->external_user, sctx->user,
                (char*)ip_or_host, server->backend_port);
        else
            conn = add_read_connection(thd, server, sctx->external_user, sctx->user, 
                (char*)ip_or_host, server->backend_port);

        if (!conn->conn_inited() && server->server_status == SERVER_STATUS_ONLINE)
        {
            if (proxy_async_connect_server) {
                // async_thd should be create in this thread, 
                // otherwise it will cause dead lock if other threads are waiting of conn inited
                if (!conn->async_thd) {
                    conn->async_thd = new THD(0);
                }
                conn->conn_async_inited = false;
                conn->conn_async_complete = false;
                pthread_t thread_id;
                conn->thd = thd;
                int error;
                if ((error = mysql_thread_create(NULL, &thread_id, NULL, 
                        proxy_reconnect_server_thread, (void*)conn)))
                {
                    mysql_mutex_unlock(&global_proxy_config.config_lock);
                    login_failed_error(thd);
                    my_error(ER_CANT_CREATE_THREAD, MYF(ME_FATALERROR), error);
                    sql_print_warning("arkproxy create server thread failed(%d)", error);
                    return true;
                }

                pthread_detach_this_thread();
                /*
                std::thread t(proxy_reconnect_server, thd, conn);
                t.detach();
                */
                // wait for all threads to complete before closing conn
                conn->conn_async_inited = true;
            } else {
                normal_count++;
                if(proxy_reconnect_server(thd, conn))
                {
                    connection_count++;
                }
            }
        }

        server_ptr = LIST_GET_NEXT(link, server_ptr);
        if (server_ptr == NULL && !entered)
        {
            entered = true;
            server_ptr = LIST_GET_FIRST(global_proxy_config.ro_server_lst);
        }
    }
    /*
    if (proxy_async_connect_server && thd->write_conn_count > 0)
    {
        for (int i = 0; !conn_available && i < thd->write_conn_count; i++)
        {
            conn = thd->write_conn[i];
            if(conn->conn_inited()) {
                conn_available = true;
                break;
            }
        }
    }
    if (proxy_async_connect_server && thd->read_conn_count > 0)
    {
        for (int i = 0; !conn_available && i < thd->read_conn_count; i++)
        {
            conn = thd->read_conn[i];
            if(conn->conn_inited()) {
                conn_available = true;
                break;
            }
        }
    }
    */
    mysql_mutex_unlock(&global_proxy_config.config_lock);

    bool conn_available = false;
    if (!proxy_async_connect_server && normal_count != connection_count) {
        conn_available = true;
    }

    if (proxy_async_connect_server)
        conn_available = true;
    /* if no ok connection build */
    if (!conn_available)
    {
        login_failed_error(thd);
        return true;
    }

    return false;
}

int check_white_ip(char* ip, int length)
{
    if (proxy_white_ips_on_update || length == 0)
        return false;
    
    proxy_white_ip_t* white_ip = LIST_GET_FIRST(global_proxy_config.white_ip_lst);
    for (uint i=0; i < LIST_GET_LEN(global_proxy_config.white_ip_lst); ++i)
    {
        if (strcmp(white_ip->ip, ip) == 0)
            return true;
        white_ip = LIST_GET_NEXT(link, white_ip);
    }
    return false;
}

