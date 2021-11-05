#include "sql_class.h"
#include "sql_connect.h"
#include "sql_parse.h"
#include "sql_repl.h"
#include "my_pthread.h"
#include "sql_trace.h"

Query_Processor_Output * process_mysql_query(THD* thd);

#if __CONSISTEND_READ__
extern consistend_cache_t global_consistend_cache;
#endif 

int proxy_is_stmt(THD* thd)
{
    if (thd->get_command() == COM_STMT_PREPARE ||
        thd->get_command() == COM_STMT_EXECUTE ||
        thd->get_command() == COM_STMT_CLOSE ||
        thd->get_command() == COM_STMT_RESET ||
        thd->get_command() == COM_STMT_FETCH ||
        thd->get_command() == COM_STMT_SEND_LONG_DATA)
        return true;
    return false;
}

int proxy_router_type(THD* thd)
{
    if (thd->get_command() == COM_STMT_PREPARE ||
        thd->get_command() == COM_STMT_EXECUTE ||
        thd->get_command() == COM_STMT_CLOSE ||
        thd->get_command() == COM_STMT_RESET ||
        thd->get_command() == COM_STMT_FETCH ||
        thd->get_command() == COM_STMT_SEND_LONG_DATA)
        return ROUTER_TYPE_RW;

    if (thd->lex->current_select &&
        thd->lex->current_select->lock_type >= TL_WRITE_ALLOW_WRITE)
        return ROUTER_TYPE_RW;

    switch (thd->lex->sql_command)
    {
    case SQLCOM_INSERT:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
    case SQLCOM_DELETE:
    case SQLCOM_DELETE_MULTI:
    case SQLCOM_CREATE_TABLE:
    case SQLCOM_CREATE_INDEX:
    case SQLCOM_ALTER_TABLE:
    case SQLCOM_TRUNCATE:
    case SQLCOM_DROP_TABLE:
    case SQLCOM_DROP_INDEX:
    case SQLCOM_SHOW_MASTER_STAT:
    case SQLCOM_CALL:
        return ROUTER_TYPE_RW;

    case SQLCOM_SELECT:
        return ROUTER_TYPE_RO;

        /* 这些SHOW操作，默认打到主库，除非显式指定打到从库 */
    case SQLCOM_SHOW_DATABASES:
    case SQLCOM_SHOW_TABLES:
    case SQLCOM_SHOW_FIELDS:
    case SQLCOM_SHOW_KEYS:
    case SQLCOM_SHOW_VARIABLES:
    case SQLCOM_SHOW_STATUS:
    case SQLCOM_SHOW_ENGINE_LOGS:
    case SQLCOM_SHOW_ENGINE_STATUS:
    case SQLCOM_SHOW_ENGINE_MUTEX:
    case SQLCOM_SHOW_PROCESSLIST:
    case SQLCOM_SHOW_SLAVE_STAT:
    case SQLCOM_SHOW_GRANTS:
    case SQLCOM_SHOW_CREATE:
    case SQLCOM_SHOW_CHARSETS:
    case SQLCOM_SHOW_COLLATIONS:
    case SQLCOM_SHOW_CREATE_DB:
    case SQLCOM_SHOW_TABLE_STATUS:
    case SQLCOM_SHOW_TRIGGERS:
        {
            if (thd->lex->route_type == ROUTER_TYPE_RO)
                return ROUTER_TYPE_RO;
            return ROUTER_TYPE_RW;
        }
        break;

    default:
        return ROUTER_TYPE_RW;
    }

    return ROUTER_TYPE_RW;
}

int proxy_connect_new_server(THD* thd)
{
    proxy_server_t* server;
    proxy_server_t* server_next;
    Security_context *sctx= thd->security_ctx;
    int i;
    backend_conn_t* conn;
    char* ip_or_host;

    ip_or_host = (char*)(sctx->ip ? sctx->ip : sctx->host_or_ip);

    for (i = 0; i < thd->read_conn_count; ++i) {
        conn = thd->read_conn[i];
        conn->server_flag = false;
    }

    global_proxy_config.config_read_lock();

    server = LIST_GET_FIRST(global_proxy_config.ro_server_lst);
    while (server)
    {
        server_next = LIST_GET_NEXT(link, server);
        if (server->server->server_status != SERVER_STATUS_ONLINE ||
            !WITH_READ_ROUTED(server->server->routed)) {
          server = server_next;
          continue;
        }

        for (i=0; i< thd->read_conn_count; ++i)
        {
            conn = thd->read_conn[i];
            if (conn->server == server->server)
                break;
        }

        /* if i is last, then conn is new connection
         * else conn is old, if it not inited, then reconnect */
        if (i == thd->read_conn_count)
        {
            conn = add_read_connection(thd, server->server, sctx->external_user, sctx->user,
                (char*)ip_or_host, server->server->backend_port);
        }

        /* if the server is not existed, add it and reconnect */
        if (conn && !conn->conn_inited()) {
          if (!proxy_reconnect_server(thd, conn)) {
            sql_print_warning(
                "Reconnecting the read server (%s, %d) successfully",
                server->server->backend_host, server->server->backend_port);
          }
        }

        server = server_next;
    }
    for (i = 0; i < thd->write_conn_count; ++i) {
        conn = thd->write_conn[i];
        conn->server_flag = false;
    }

    server = LIST_GET_FIRST(global_proxy_config.rw_server_lst);
    while (server)
    {
        server_next = LIST_GET_NEXT(link, server);
        if (server->server->server_status != SERVER_STATUS_ONLINE ||
            !WITH_WRITE_ROUTED(server->server->routed)) {
          server = server_next;
          continue;
        }

        for (i=0; i< thd->write_conn_count; ++i)
        {
            conn = thd->write_conn[i];
            if (conn->server == server->server)
                break;
        }
        /* if i is last, then conn is new connection
        * else conn is old, if it not inited, then reconnect */
        if (i == thd->write_conn_count)
        {
            conn = add_write_connection(thd, server->server, sctx->external_user, sctx->user,
                (char*)ip_or_host, server->server->backend_port);
        }
        conn->server_flag = true;
        if (conn && !conn->conn_inited()) {
          if (!proxy_reconnect_server(thd, conn)) {
            sql_print_warning(
                "Reconnecting the write server (%s, %d) successfully",
                server->server->backend_host, server->server->backend_port);
          }
        }

        server = server_next;
    }

    global_proxy_config.config_unlock();

    int start = 0;
    int end = thd->read_conn_count - 1;
    while (start <= end) {
      conn = thd->read_conn[start];
      if (conn->server_flag == false) {
        close_backend_connection(conn);
        my_free(conn);
        thd->read_conn[start] = thd->read_conn[end];
        thd->read_conn[end] = nullptr;
        end--;
        thd->read_conn_count--;
      } else {
        start++;
      }
    }
    start = 0;
    end = thd->write_conn_count - 1;
    while (start <= end) {
      conn = thd->write_conn[start];
      if (conn->server_flag == false) {
        close_backend_connection(conn);
        my_free(conn);
        thd->write_conn[start] = thd->write_conn[end];
        thd->write_conn[end] = nullptr;
        end--;
        thd->write_conn_count--;
      } else {
        start++;
      }
    }

    return false;
}

backend_conn_t* proxy_route_stmt(THD* thd)
{
    stmtid_conn_t* stmtid_conn;

    stmtid_conn = LIST_GET_FIRST(thd->stmtid_cache->stmtid_lst);
    while (stmtid_conn)
    {
        if (stmtid_conn->client_stmt_id == thd->stmt_id)
        {
            int4store(thd->query(), stmtid_conn->stmt_id);
            return stmtid_conn->stmt_conn;
        }

        stmtid_conn = LIST_GET_NEXT(link, stmtid_conn);
    }

    return NULL;
}

int proxy_router_temp_table(THD* thd)
{
    if (thd->lex->sql_command != SQLCOM_CREATE_TABLE)
        return false;

    HA_CREATE_INFO create_info(thd->lex->create_info);
    HA_CREATE_INFO* create_info_ptr = &create_info;

    if (create_info_ptr->options & HA_LEX_CREATE_TMP_TABLE)
        return true;

    return false;
}

#if __CONSISTEND_READ__
int proxy_consistend_delay(
    backend_conn_t* conn, 
    char* dbname,
    char* tablename,
    longlong update_time)
{
    proxy_consistend_read_t* proxy_consistend;
    mysql_mutex_lock(&conn->consistend_cache->consistend_lock);
    proxy_consistend = LIST_GET_FIRST(conn->consistend_cache->consistend_read_lst);
    while (proxy_consistend)
    {
        if (!strcasecmp(proxy_consistend->dbname, dbname) &&
            !strcasecmp(proxy_consistend->tablename, tablename))
        {
            if ((uint)(update_time - proxy_consistend->timestamp) > 
                proxy_consistend_max_delay_time * 1000000)
            {
                mysql_mutex_unlock(&conn->consistend_cache->consistend_lock);
                return true;
            }

            mysql_mutex_unlock(&conn->consistend_cache->consistend_lock);
            return false;
        }

        proxy_consistend = LIST_GET_NEXT(link, proxy_consistend);
    }

    mysql_mutex_unlock(&conn->consistend_cache->consistend_lock);

    /* if not found the tables, then considered to sync delay */
    return true;
}

int proxy_route_consistend_tables_delay(THD* thd, backend_conn_t* conn)
{
    TABLE_LIST*     table;

    if (!conn->consistend_cache)
        return false;

    mysql_mutex_lock(&global_consistend_cache.consistend_lock);
    for (table=thd->lex->query_tables; table; table=table->next_global)
    {
        proxy_consistend_read_t* proxy_consistend;
        
        proxy_consistend = LIST_GET_FIRST(global_consistend_cache.consistend_read_lst);
        while (proxy_consistend)
        {
            if (!strcasecmp(proxy_consistend->dbname, table->db) &&
                !strcasecmp(proxy_consistend->tablename, table->table_name))
            {
                if (proxy_consistend_delay(conn, table->db, 
                    table->table_name, proxy_consistend->timestamp))
                {
                    mysql_mutex_unlock(&global_consistend_cache.consistend_lock);
                    return true;
                }

                break;
            }

            proxy_consistend = LIST_GET_NEXT(link, proxy_consistend);
        }
    }

    mysql_mutex_unlock(&global_consistend_cache.consistend_lock);

    return false;
}

#endif

backend_conn_t* 
proxy_router_command(THD* thd)
{
    backend_conn_t* conn;

    if (thd->config_version < global_proxy_config.config_version)
    {
        proxy_connect_new_server(thd);
        thd->config_version = global_proxy_config.config_version;
        /* if reconfig, then set nosense of temp conn, if the 
         * command route to other node, then raise error */
        thd->last_temp_conn = NULL;
    }

    /* router here*/
    if (proxy_not_router_mode)
    {
        conn = get_write_connection(thd);
    }
    else if (thd->last_temp_conn != NULL)
    {
        conn = thd->last_temp_conn;
    }
    else if (thd->last_trx_conn != NULL)
    {
        conn = thd->last_trx_conn;
        if (proxy_router_temp_table(thd))
        {
            /* If a temporary table created in transaction */
            thd->last_temp_conn = conn;
        }
    }
    else if (proxy_router_temp_table(thd))
    {
        /* If a temporary table created */
        conn = proxy_route_to_first_write(thd);
        thd->last_temp_conn = conn;
    }
    else if (thd->lex->sql_command == SQLCOM_BEGIN || proxy_is_stmt(thd))
    {
        /* If a transaction is start or stmt handle */
        conn = proxy_route_to_first_write(thd);
        thd->last_trx_conn = conn;
    }
    else if (proxy_router_type(thd) == ROUTER_TYPE_RW ||
        thd->server_status & SERVER_MORE_RESULTS_EXISTS)
    {
        /* if it is a multi query, then route to write only, 
         * if they are select all, can been routed to read node */
        conn = get_write_connection(thd);
    }
    else
    {
        if (thd->lex->route_type == ROUTER_TYPE_RW)
        {
            conn = get_write_connection(thd);
        }
        else
        {
            conn = get_read_connection(thd);
            if (conn == NULL)
                conn = get_write_connection(thd);
#if __CONSISTEND_READ__
            else if (proxy_route_consistend_tables_delay(thd, conn))
            {
                conn = get_write_connection(thd);
            }
#endif
        }
    }

    /* if the transaction is committed/rollbacked, then set the last to null*/
    if (thd->lex->sql_command == SQLCOM_COMMIT ||
        thd->lex->sql_command == SQLCOM_ROLLBACK)
        thd->last_trx_conn = NULL;

    if (conn)
    {
        conn->qps++;
        if (thd->db)
            strcpy(conn->db, thd->db);
    }

    return conn;
}

int proxy_binlog_dump_process(THD* thd)
{
    ulong pos;
    String slave_uuid;
    ushort flags= 0;
    char* packet;
    uint32 slave_server_id;

    packet = (char*)thd->query();
    if (thd->get_command() == COM_BINLOG_DUMP)
    {
        /*
        	4 bytes is too little, but changing the protocol would break
        	compatibility.  This has been fixed in the new protocol. @see
        	com_binlog_dump_gtid().
        */

        /* TODO: The following has to be changed to an 8 byte integer */
        pos = uint4korr(packet);
        flags = uint2korr(packet + 4);
        thd->variables.server_id=0; /* avoid suicide */
        if ((slave_server_id= uint4korr(packet+6))) // mysqlbinlog.server_id==0
            kill_zombie_dump_threads(slave_server_id);
        thd->variables.server_id = slave_server_id;
        // sql_print_information("proxy_binlog_dump_process kill %d", slave_server_id);
        // READ_INT(pos, 4);
        // READ_INT(flags, 2);
        // READ_INT(thd->server_id, 4);
        
        // get_slave_uuid(thd, &slave_uuid);
        // kill_zombie_dump_threads(&slave_uuid);
    }

    return false;
}

int proxy_check_command(THD* thd)
{
    int ret = 0;
    switch (thd->lex->sql_command)
    {
        /* not support this command */
    case SQLCOM_SLAVE_ALL_START:
    case SQLCOM_SLAVE_ALL_STOP:
    case SQLCOM_SLAVE_START:
    case SQLCOM_SLAVE_STOP:
    case SQLCOM_CHANGE_MASTER:
        ret = true;
        break;
    default:
        ret = false;
        break;
    }

    if (ret)
    {
        my_error(ER_PROXY_NOT_SUPPORT, MYF(0));
    }

    return ret;
}

int proxy_kill_enqueue(THD* thd)
{
    if (thd->lex->route_type == ROUTER_TYPE_CONNECTION_BACKEND)
    {
        return proxy_kill_push_queue_backend(thd);
    }
    else
    {
        return proxy_kill_push_queue(thd);
    }
}

int proxy_rules_check(THD* thd)
{
    Query_Processor_Output* qpo;
    int ret = false;

    if (thd->connection_type != PROXY_CONNECT_PROXY || !proxy_digest_cache_on())
        return false;

    /* include COM_QUERY and COM_PREPARE */
    /* if the digest cache is inited, then generate the digest */
    proxy_format_to_queue_before(thd);

    /* check the rules here */
    qpo = process_mysql_query(thd);
    if (qpo == NULL)
    {
        ret = false;
        goto clean;
    }

    if (qpo->new_query)
    {
        thd->set_query((char*)qpo->new_query->data(), qpo->new_query->length());
    }

    if (qpo->OK_msg)
    {
        my_ok(thd);
        ret = true;
        goto clean;
    }

    if (qpo->error_msg)
    {
        my_error(ER_NOT_ALLOWED_COMMAND, MYF(0), qpo->error_msg);
        ret = true;
        goto clean;
    }

    if (qpo->destination)
    {
        thd->lex->route_type = qpo->destination;
    }

    if (qpo->log && thd->trace_queue != NULL)
    {
        format_cache_node_t* node = &thd->trace_queue->trace_array[thd->trace_queue->enqueue_index];
        node->rule_logged = true;
    }

    ret = false;

clean:
    ///* rule cache have been used, now clean it */
    //if (thd->query_rule && thd->query_rule->digest_text)
    //{
    //    my_free(thd->query_rule->digest_text);
    //    thd->query_rule->digest_text = NULL;
    //    thd->query_rule->digest = 0;
    //}
    return ret;
}

int proxy_dispatch_query_proxy(THD* thd)
{
    backend_conn_t* conn;

    net_queue_t* queue = &thd->queue_in;
    /* clean the not complete sql packet, start from zero */
    if (queue->dequeue_index != queue->enqueue_index)
        queue->dequeue_index = queue->enqueue_index;

    if (thd->lex->sql_command == SQLCOM_CHANGE_DB &&
        thd->lex->select_lex.db != NULL)
    {
        thd->set_db(thd->lex->select_lex.db, strlen(thd->lex->select_lex.db));
    }

    /* check the rules here */
    if (proxy_rules_check(thd))
        return false;

    set_ark_env_for_connect(thd);
    
    if (proxy_check_command(thd))
        return true;
    /* if we found the connection is not inited, the we should
     * test the reconfig, and then find a new connection in pool
     * if no available connection, the report ER_NO_BACKEND_FOUND, 
     * but if the ER_NO_BACKEND_FOUND is reasonable, because it is 
     * not the MySQL error code */
    thd->clear_error();
    /* if need reconfig, then reconnect */
    if (thd->reconfig)
    {
        proxy_reconnect_servers(thd);
        thd->reconfig = false;
        thd->last_trx_conn = NULL;
    }

#if __CONSISTEND_READ__
    thd->consistend_execute_time = my_hrtime().val;
#endif

    /* here, we should get a good connection(up to now) */
    if ((conn = proxy_router_command(thd))== NULL)
    {
        Security_context *sctx= thd->security_ctx;
        my_error(ER_NEW_ABORTING_CONNECTION, MYF(0),
            thd->thread_id,
            thd->db ? thd->db : "unconnected",
            sctx->user ? sctx->user : "unauthenticated",
            sctx->host_or_ip, "Can not find available backend mysql server");
        return true;
    }

    if (thd->last_conn != conn)
    {
        proxy_env_option_push_queue(thd, conn);
    }

    thd->last_conn = conn;
    proxy_binlog_dump_process(thd);

    /* wait the SQL execute completed, when error, then process it 
     * according to the strategy of proxy error, retry or others */
    
    if (proxy_kill_enqueue(thd))
        return false;

    if (thd->query() == NULL) {
      sql_print_warning("empty query found");
      my_ok(thd);
      return false;
    }
    if (mysql_push_execute_queue(thd, conn))
    {
      /* cant to retry here, because the message partly send to client already,
       * if here to retry, then send partly message again, then "Malformed packet"
        if (++retry_count < proxy_fail_retry_count && 
            thd->get_command() != COM_BINLOG_DUMP)
        {
            thd->get_stmt_da()->reset_diagnostics_area();
            thd->get_stmt_da()->disable_status();
            goto retry;
        }
      */

        net_send_error(thd, thd->get_stmt_da()->sql_errno(), 
            thd->get_stmt_da()->message(),
            thd->get_stmt_da()->get_sqlstate());
        thd->get_stmt_da()->reset_diagnostics_area();
        thd->get_stmt_da()->disable_status();
    }

    /* rule cache have been used, now clean it */
    if (thd->query_rule)
    {
        if (thd->query_rule->digest_text)
        {
            my_free(thd->query_rule->digest_text);
            thd->query_rule->digest_text = NULL;
        }

        if (thd->query_rule->qpo)
        {
            my_free(thd->query_rule->qpo);
            thd->query_rule->qpo = NULL;
        }

        thd->query_rule->digest = 0;
    }

    return false;
}

int proxy_dispatch_query_shell(THD* thd)
{
    proxy_shell_dispatch(thd);
    return false;
}

int proxy_dispatch_query(THD* thd)
{
    if (thd->connection_type == PROXY_CONNECT_PROXY)
    {
        if (thd->lex->sql_command >= SQLCOM_CONFIG && 
            thd->lex->sql_command < SQLCOM_END)
        {
            my_error(ER_SHELL_COMMAND_ON_PROXY, MYF(0));
            return true;
        }
        
        if (thd->lex->sql_command == SQLCOM_LOAD)
        {
            my_error(ER_UNKNOWN_COM_ERROR, MYF(0));
            return true;
        }

        proxy_dispatch_query_proxy(thd);
    }
    else
    {
        if (thd->lex->sql_command < SQLCOM_CONFIG
            && thd->lex->sql_command != SQLCOM_SHOW_VARIABLES
            && thd->lex->sql_command != SQLCOM_SET_OPTION)
        {
            my_error(ER_SQL_COMMAND_ON_SHELL, MYF(0));
            return true;
        }

        proxy_dispatch_query_shell(thd);
    }

    return false;
}

void proxy_mysql_parse(
    THD *thd, char *rawbuf, uint length,
    Parser_state *parser_state,
    bool is_com_multi,
    bool is_next_command
)
{
    DBUG_ENTER("proxy_mysql_parse");
    int qlen;

    lex_start(thd);
    thd->reset_for_next_command();
    if (is_next_command)
    {
        thd->server_status|= SERVER_MORE_RESULTS_EXISTS;
        if (is_com_multi)
            thd->get_stmt_da()->set_skip_flush();
    }

    LEX *lex= thd->lex;

    bool err= parse_sql(thd, parser_state, NULL, true);
    if (!err)
    {
        if (! thd->is_error())
        {
            const char *found_semicolon= parser_state->m_lip.found_semicolon;
            /* Actually execute the query */
            general_log_write(thd, thd->get_command(), thd->query(), thd->query_length());
            if (found_semicolon)
            {
                if (found_semicolon && (ulong) (found_semicolon - thd->query()))
                    thd->set_query(thd->query(), (uint32) (found_semicolon - thd->query() - 1),
                                   thd->charset());
                lex->safe_to_cache_query= 0;
                thd->server_status|= SERVER_MORE_RESULTS_EXISTS;
            }

            proxy_dispatch_query(thd);
        }
    }
    else
    {
        /* Instrument this broken statement as "statement/sql/error" */
        DBUG_ASSERT(thd->is_error());
    }

    /* move this two line here, to imporve the performance of arkproxy */
    thd->end_statement();
    thd->cleanup_after_query();
    //THD_STAGE_INFO(thd, stage_freeing_items);
    DBUG_VOID_RETURN;
}

bool proxy_execute_query_command(
    enum enum_server_command command, 
    THD *thd,
    char* packet, 
    uint packet_length, 
    bool is_com_multi,
    bool is_next_command
)
{
    int origin_length;
    if (alloc_query(thd, packet, packet_length))
        return false;

    Parser_state parser_state;
    if (parser_state.init(thd, thd->query(), thd->query_length()))
        return false;

    char *packet_end= thd->query() + thd->query_length();
    origin_length =thd->query_length();

    proxy_mysql_parse(thd, thd->query(), thd->query_length(), &parser_state,
                is_com_multi, is_next_command);

    while (!thd->killed && (parser_state.m_lip.found_semicolon != NULL) &&
           ! thd->is_error() /*&& !(thd->server_status & SERVER_MORE_RESULTS_EXISTS)*/)
    {
        thd->update_server_status();
        thd->protocol->end_statement();
        thd->get_stmt_da()->set_skip_flush();
        /*
          Multiple queries exist, execute them individually
        */
        char *beginning_of_next_stmt= (char*) parser_state.m_lip.found_semicolon;

        ulong length= (ulong)(packet_end - beginning_of_next_stmt);

        /* Remove garbage at start of query */
        while (length > 0 && my_isspace(thd->charset(), *beginning_of_next_stmt))
        {
            beginning_of_next_stmt++;
            length--;
        }

        thd->set_query_and_id(beginning_of_next_stmt, length,
                              thd->charset(), next_query_id());
        //thd->set_time(); /* Reset the query start time. */
        parser_state.reset(beginning_of_next_stmt, length);
        proxy_mysql_parse(thd, beginning_of_next_stmt, length, &parser_state,
                    is_com_multi, is_next_command);
    }

    //if (!thd->is_error() && (thd->server_status & SERVER_MORE_RESULTS_EXISTS))
    //{
    //    thd->set_query_and_id(packet, (ulong)origin_length,
    //                          thd->charset(), next_query_id());
    //    proxy_dispatch_query(thd);
    //}

    return false;
}

