#include "sql_class.h"
#include "sql_connect.h"
#include "sql_common.h"
#include "errmsg.h"
#include "ark_config.h"
#include "sql_parse.h"
#include "sql_prepare.h"
#include "sql_trace.h"
#include "sql_rules.h"

extern MYSQL* get_backend_connection(THD* thd, backend_conn_t* conn);
void __reset_rules(std::vector<QP_rule_t *> * qrs);
extern int proxy_set_the_timeout(THD* thd, backend_conn_t* conn, int64_t exe_time_1);
#if __CONSISTEND_READ__
extern consistend_cache_t global_consistend_cache;
int mysql_fetch_slave_consistend_read_table(backend_conn_t* conn);
int proxy_update_consistend_cache(
    consistend_cache_t* consistend_cache,
    int instance,
    char* dbname,
    char* tablename,
    longlong update_time
);
#endif

const char* net_state_array[]=
{
    "",
    "net_meta_eof",
    "net_eof",
    "net_full",
    "net_null",
    "net_error",
    "net_ok",
    "net_gen",
    "net_full_end",
    "net_empty",
    "net_meta_eof_end",
    "net_skip",
    "net_eof_non_end",
    "net_eof_end_flush",
};

#ifndef DBUG_OFF
//#if 0
#define PRINT_PACKET(thd, packet, net_state) while (0){}
#else
#define command_len 50

int proxy_print_packet(THD* thd, net_packet_t* packet, int net_state)
{
    int to = 0, i, len;
    char* p;
    char command_tmp[command_len];

    if (proxy_print_net_queue == 0)
        return false;

    if (net_state == NET_STATE_ERROR)
        sql_print_information("errmsg: %s", packet->message);

    if (str_get_len(&packet->buffer) > 0 && packet->sub_net_state == 0)
    {
        int com_size = command_len > str_get_len(&packet->buffer) ? 
          str_get_len(&packet->buffer) : command_len;
        memcpy(command_tmp, str_get(&packet->buffer), com_size);
        command_tmp[com_size] = 0;
    }
    else if (packet->sub_net_state != 0)
    {
        memset(command_tmp, 0, command_len);
        p = str_get(&packet->buffer);
        len = str_get_len(&packet->buffer);
        for (i=0; i < len; i++)
            to += (int)p[i];

        sprintf(command_tmp, "ack(%d)", to);
    }
    else
        strcpy(command_tmp, "null");

    sql_print_information("thd: %lld, packet: type(%s), com(%d), "
        "command(%s), length(%d), status: %d", 
        thd->thread_id, net_state_array[net_state], packet->command, command_tmp, 
        str_get_len(&packet->buffer), packet->server_status);
    return false;
}

#define PRINT_PACKET(thd, packet, net_state) proxy_print_packet(thd, packet, net_state)
#endif

str_t*
str_init(str_t* str)
{
    str->str = str->str_buf;
    str->str_len = NAME_CHAR_LEN;
    str->cur_len = 0;
    str->extend_len = 0;
    str->last_len = 0;
    memset(str->str, 0, NAME_CHAR_LEN);

    return str;
}

str_t*
str_init_with_extend(str_t* str, int extend_len)
{
    str->str = str->str_buf;
    str->str_len = NAME_CHAR_LEN;
    str->cur_len = 0;
    str->extend_len = extend_len;
    str->last_len = 0;
    memset(str->str, 0, NAME_CHAR_LEN);

    return str;
}

str_t*
str_relloc(str_t* str, int namelen)
{
    char*   select_item_tmp;
    int    buflen ;
    int    newlen ;

    if(str->extend_len > 0)
        newlen = namelen + str->extend_len;
    else
        newlen = namelen + STR_EXTEND_LENGTH;
    str->last_len += (newlen - namelen);
    select_item_tmp = (char*)my_malloc(newlen, MYF(0));
    memcpy(select_item_tmp, str->str, str->cur_len);	
    buflen = newlen;
    if (str->str != str->str_buf)
        my_free(str->str);

    str->str_len = buflen;
    str->str = select_item_tmp;
    return str;
}

/*only use to append one char*/
str_t*
str_append_1(
    str_t*  str,
    const char* new_string
)
{
    if (str->cur_len + 10 >= str->str_len)
        str_relloc(str, str->cur_len + 10);

    str->str[str->cur_len] = new_string[0];
    str->cur_len ++;
    return str;
}

str_t *packet_buffer_str_truncate_0(THD *thd, net_queue_t *queue, str_t *str) {
  volatile int64 *buf_size = &thd->queue_packet_buf_size;
  volatile int64 *total_size;
  int64 max_len;
  int queue_size;
  if (queue == &thd->queue_in) {
    total_size = &thd->queue_in_packet_total_len;
    queue_size = thd->thd_proxy_recv_queue_size;
    max_len = proxy_net_buffer_outlier_scale * *total_size / queue_size;
  } else {
    total_size = &thd->queue_out_packet_total_len;
    queue_size = thd->thd_proxy_send_queue_size;
    max_len = proxy_net_buffer_outlier_scale * *total_size / queue_size;
  }
  if (proxy_net_per_buffer_max_size > 0 &&
      proxy_net_per_buffer_max_size < max_len) {
    max_len = proxy_net_per_buffer_max_size;
  }

  // str len超过最大值的情况可能是毛刺或者平均值减小后需要收缩
  if (str->str_len > (max_len + STR_EXTEND_LENGTH)) {
    int old_len = str->str_len;
    my_free(str->str);
    str->str = str->str_buf;
    str->str_len = NAME_CHAR_LEN;
    if (str->str_len != old_len) {
      my_atomic_add64(buf_size, str->str_len - old_len);
    }
  }
  int new_cur_len = str->cur_len;
  //过小的数据包不做统计
  if (new_cur_len >= NAME_CHAR_LEN) {
    my_atomic_add64(total_size, new_cur_len - *total_size / queue_size);
  }
  str->cur_len = 0;
  return str;
}

str_t *packet_buffer_str_append(THD *thd, str_t *str, const char *new_string,
                                int len) {
  volatile int64 *buf_size = &thd->queue_packet_buf_size;
  int old_len = str_get_alloc_len(str);
  str_append_with_length(str, new_string, len);
  int new_len = str_get_alloc_len(str);
  if (new_len != old_len) {
      my_atomic_add64(buf_size, new_len - old_len);
  }
  return str;
}

str_t*
str_append_with_length(
    str_t*  str,
    const char* new_string,
    int len
)
{
    int    namelen;
    if(new_string == NULL || len ==0) return str;

    namelen = str->cur_len + len + 1;
    if (namelen >= str->str_len)
        str_relloc(str, namelen);

    memcpy(str->str + str->cur_len, new_string, len);
    str->cur_len += len;
    return str;
}

str_t*
str_append(
    str_t*  str,
    const char* new_string
)
{
    return str_append_with_length(str, new_string, strlen(new_string));
}

str_t*
str_truncate_0(str_t* str)
{
    str->cur_len = 0;
    return str;
}

str_t*
str_truncate(str_t* str, int endlen)
{
    if (endlen >= str->cur_len)
        endlen = str->cur_len;

    str->cur_len -= endlen;
    return str;
}

void
str_reset_last_len(str_t* str)
{
    if (str == NULL)
        return;
    str->last_len = 0;
}

void
str_deinit(str_t* str)
{
    if (str == NULL)
        return;

    if (str->str != str->str_buf)
    {
        my_free(str->str);
        str->str = str->str_buf;
    }
}

char*
str_get(str_t* str)
{
    return str->str;
}

int
str_get_len(str_t* str)
{
    return str->cur_len;
}

int
str_get_alloc_len(str_t* str)
{
    return str->str_len;
}

int
str_get_last_len(str_t* str)
{
    return str->last_len;
}

int
str_set_last_len(str_t* str)
{
    return str->last_len = str->str_len;
}

int
proxy_send_query(MYSQL* mysql, const char* query, ulong length,
                 enum_server_command com_type, my_bool skip_check)
{
    DBUG_ENTER("proxy_send_query");
    if (com_type == COM_PING)
        DBUG_RETURN(simple_command(mysql, com_type, 0, 0, skip_check));
    else
        DBUG_RETURN(simple_command(mysql, com_type, (uchar*) query, length, skip_check));
}

ulong mysql_net_read(MYSQL* mysql, int* real_len)
{
    ulong len = 0;
    DBUG_ENTER("mysql_read_event");

    ulong reallen = 0;
    NET* net;

    net = &mysql->net;
    if (net->vio != 0)
        len= my_net_read_packet_reallen(&mysql->net, 0, &reallen);
    // len = cli_safe_read(mysql);
    if (len == packet_error || (long) len < 1)
    {
        DBUG_RETURN(packet_error);
    }

    /* Check if eof packet */
    if (len < 8 && mysql->net.read_pos[0] == 254)
    {
        if (len > 1)/* MySQL 4.1 protocol */
        {
            mysql->warning_count= uint2korr(mysql->net.read_pos+1);
            mysql->server_status= uint2korr(mysql->net.read_pos+3);
            *real_len = len;
        }

        DBUG_RETURN(0);
    }

    DBUG_RETURN(len);
}

int net_push_signal(THD* thd, int net_state, int force)
{
    mysql_mutex_lock(&thd->read_lock);
    mysql_cond_signal(&thd->read_cond);
    mysql_mutex_unlock(&thd->read_lock);
    ++thd->cond_signal_count;
    return false;

    if (force)
    {
        return false;
    }

    switch (net_state)
    {
    case NET_STATE_FULL_END:
    case NET_STATE_OK:
    case NET_STATE_META_EOF_END:
    case NET_STATE_EOF:
    case NET_STATE_EOF_NONEND:
    case NET_STATE_END_FLUSH:
        {
            mysql_mutex_lock(&thd->read_lock);
            mysql_cond_signal(&thd->read_cond);
            mysql_mutex_unlock(&thd->read_lock);
        }
        break;
    default:
        break;
    }
}

net_packet_t* 
net_queue_get_node(THD* thd, net_queue_t* queue, int queue_size, net_packet_t* inherit)
{
    int dequeue_index;
    int enqueue_index;
    net_packet_t* packet;

retry:
    dequeue_index = queue->dequeue_index;
    enqueue_index = queue->enqueue_index;
    if ((enqueue_index+1) % queue_size != dequeue_index)
    {
        packet = &queue->packets[enqueue_index];
        queue->enqueue_index = (enqueue_index + 1) % queue_size;
        packet_buffer_str_truncate_0(thd, queue, &packet->buffer);
        packet->command = COM_END;
        packet->sub_command = SQLCOM_END;
        packet->sub_net_state = 0;
        packet->server_status = 0;
        memset(&packet->ok_info, 0, sizeof(ok_info_struct));
        if (inherit)
        {
            packet->command = inherit->command;
            packet->sub_command = inherit->sub_command;
            packet->sub_net_state = inherit->net_state;
        }

        packet->net_state = NET_STATE_NULL;
        return packet;
    }
    else
    {
        //queue is full, wait to consume
        // if the queue come from mysqld, then notify to comsume the message
        // if (&thd->queue_in == queue)
        /* 这里解决的问题是，如果recv queue满了，则会进入这个死循环，但语句
         入执行队列的时候，需要executer_lock加锁，而此时这里已经进入死循环，
         则加不上锁，导致执行线程和连接线程之间进入了死锁阶段，而死循环的时候
         这个线程会导致一个CPU核达到100% */
        mysql_mutex_unlock(&thd->executer_lock);
        net_push_signal(thd, 0, true);
        mysql_mutex_lock(&thd->executer_lock);
        goto retry;
    }

    return NULL;
}

int
net_error_enqueue(MYSQL* mysql, THD* thd, THD* exe_thd, size_t len, net_packet_t* enpacket)
{
    net_packet_t* packet;
    NET* net;
    net = &mysql->net;

    if (len > 3)
    {
        char *pos=(char*) net->read_pos+1;
        net->last_errno=uint2korr(pos);
        pos+=2;
        len-=2;
        
        if (protocol_41(mysql) && pos[0] == '#')
        {
            strmake(net->sqlstate, pos+1, SQLSTATE_LENGTH);
            pos+= SQLSTATE_LENGTH+1;
        }
        (void) strmake(net->last_error,(char*) pos,
        MY_MIN((uint) len,(uint) sizeof(net->last_error)-1));
    }
    else
    {
        set_mysql_error(mysql, CR_UNKNOWN_ERROR, unknown_sqlstate);
    }

    mysql->server_status&= ~SERVER_MORE_RESULTS_EXISTS;

    packet = net_queue_get_node(thd, &thd->queue_in, thd->thd_proxy_recv_queue_size, enpacket);
    packet->error_code = mysql_errno(mysql);
    my_message(mysql_errno(mysql), mysql_error(mysql), MYF(0));
    strcpy(packet->message, exe_thd->get_stmt_da()->message());
    exe_thd->clear_error();
    packet_buffer_str_append(thd, &packet->buffer,
                             (const char *)mysql->net.read_pos, len);
    PRINT_PACKET(thd, packet, NET_STATE_ERROR);
    packet->net_state = NET_STATE_ERROR;
    enpacket->net_state = NET_STATE_SKIP;

    return false;
}

void free_old_ok_info(net_packet_t* packet)
{
    packet->ok_info.affected_rows= 0;
    packet->ok_info.insert_id= 0;
    packet->ok_info.server_status= 0;
    packet->ok_info.warning_count= 0;
    memset(packet->message, 0, sizeof(packet->message));
    return;
}

my_bool proxy_read_ok_info(MYSQL *mysql, ulong length, net_packet_t* packet)
{
    uchar *pos;
    ulong field_count;
    DBUG_ENTER("proxy_read_ok_info");
    
    free_old_ok_info(packet);		/* Free old result */

    pos=(uchar*) mysql->net.read_pos;
    if ((field_count= net_field_length(&pos)) == 0)
    {
        packet->ok_info.affected_rows= net_field_length_ll(&pos);
        packet->ok_info.insert_id=	  net_field_length_ll(&pos);
        if (protocol_41(mysql))
        {
            packet->ok_info.server_status=uint2korr(pos); pos+=2;
            packet->ok_info.warning_count=uint2korr(pos); pos+=2;
        }
        else if (mysql->server_capabilities & CLIENT_TRANSACTIONS)
        {
            /* MySQL 4.0 protocol */
            packet->ok_info.server_status=uint2korr(pos); pos+=2;
            packet->ok_info.warning_count= 0;
        }

        mysql->server_status = packet->ok_info.server_status;
        if (pos < mysql->net.read_pos+length && net_field_length(&pos))
            strcpy(packet->message, (char*)pos);
    }

    DBUG_RETURN(0);
}

int
net_read_push_queue_noloop(
    THD* thd, 
    THD* exe_thd, 
    MYSQL* mysql, 
    net_packet_t* enpacket, 
    int net_state,
    int *reconnect
)
{
    size_t len;
    net_packet_t* packet;
    NET* net;
    net = &mysql->net;
    int real_len = 0;
    ulong field_count;
    uchar *pos;

    len = mysql_net_read(mysql, &real_len);
    if (len != 0 && len != packet_error)	
    {
        if (mysql->net.read_pos[0] ==255)
        {
            net_error_enqueue(mysql, thd, exe_thd, len, enpacket);
        }
        else
        {
            mysql->warning_count= 0;
            pos= (uchar*) mysql->net.read_pos;
            enpacket->field_count = 0;
            field_count= net_field_length(&pos);
            enpacket->field_count = field_count;
            if (enpacket->command == COM_STMT_PREPARE)
            {
                uchar *pos;
                mysql->warning_count= 0;
                pos= (uchar*) mysql->net.read_pos;
                enpacket->stmt_id= uint4korr(pos+1);
                /* modify the stmtid */
                //int4store(pos + 1,thd->statement_id_counter & STMT_ID_MASK);
                pos+= 5;
                /* Number of columns in result set */
                enpacket->field_count = net_field_length(&pos);
                /* Number of placeholders in the statement */
                enpacket->param_count=   uint2korr(pos);   pos+= 2;
                // if (packet_length >= 12)
                mysql->warning_count= uint2korr(pos+1);
                if (enpacket->field_count || enpacket->param_count)
                    net_state = NET_STATE_FULL;
                else
                    net_state = NET_STATE_FULL_END;
            }
            else if (field_count == 0)
            {
                enpacket->field_count = field_count;
                if (enpacket->field_count)
                    net_state = NET_STATE_FULL;
                else
                    net_state = NET_STATE_FULL_END;
            }

            if (enpacket->command != COM_PING || enpacket->net_state != NET_STATE_GEN)
            {
                packet = net_queue_get_node(thd, &thd->queue_in, thd->thd_proxy_recv_queue_size, enpacket);
                packet_buffer_str_append(thd, &packet->buffer,
                                         (const char *)mysql->net.read_pos,
                                         len);
                if (enpacket->field_count == 0)
                    proxy_read_ok_info(mysql, len, packet);
                if ((net_state== NET_STATE_OK || net_state== NET_STATE_EOF ||
                      net_state== NET_STATE_META_EOF || 
                      net_state== NET_STATE_FULL_END || 
                      net_state== NET_STATE_META_EOF_END) && 
                    enpacket->server_status & SERVER_MORE_RESULTS_EXISTS)
                    int2store(str_get(&packet->buffer) + 3, 
                        mysql->server_status | SERVER_MORE_RESULTS_EXISTS);
                packet->server_status = mysql->server_status;
                PRINT_PACKET(thd, packet, net_state);
                packet->net_state = net_state;
            }
        }
    }
    else
    {
        if (enpacket->command != COM_PING || enpacket->net_state != NET_STATE_GEN)
        {
            if ((mysql_errno(mysql) == 1158 ||
                 mysql_errno(mysql) == 1152 ||
                 mysql_errno(mysql) == 1156 ||
                 mysql_errno(mysql) == 1159 ||
                 mysql_errno(mysql) == 1160 ||
                 mysql_errno(mysql) == 1161 ||
                 mysql_errno(mysql) == 1184 ||
                 mysql_errno(mysql) == 2013 ||
                 mysql_errno(mysql) == 2014 ||
                 mysql_errno(mysql) == 2003 ||
                mysql_errno(mysql) == 2006)
                && *reconnect == false)
            {
                sql_print_warning("arkproxy network error(%d), reconnect the backend server", 
                    mysql_errno(mysql));
                exe_thd->clear_error();
                *reconnect = true;
                return true;
            }

            packet = net_queue_get_node(thd, &thd->queue_in, thd->thd_proxy_recv_queue_size, enpacket);
            packet->error_code = mysql_errno(mysql);
            my_message(mysql_errno(mysql), mysql_error(mysql), MYF(0));
            strcpy(packet->message, exe_thd->get_stmt_da()->message());
            exe_thd->clear_error();
            PRINT_PACKET(thd, packet, NET_STATE_ERROR);
            packet->net_state = NET_STATE_ERROR;
        }
        else
        {
            enpacket->error_code = mysql_errno(mysql);
            my_message(mysql_errno(mysql), mysql_error(mysql), MYF(0));
            strcpy(enpacket->message, exe_thd->get_stmt_da()->message());
            exe_thd->clear_error();
            enpacket->net_state = NET_STATE_ERROR;
        }
    }

    net_push_signal(thd, net_state, false);
    return false;
}


int
net_noread_push_queue(THD* thd, THD* exe_thd, MYSQL* mysql, net_packet_t* enpacket, int net_state)
{
    net_packet_t* packet;
    NET* net;
    net = &mysql->net;
    packet = net_queue_get_node(thd, &thd->queue_in, thd->thd_proxy_recv_queue_size, enpacket);
    str_append_with_length(&packet->buffer, "", 0);
    proxy_read_ok_info(mysql, 0, packet);
    PRINT_PACKET(thd, packet, net_state);
    packet->net_state = net_state;
    
    net_push_signal(thd, net_state, false);
    return false;
}

int
net_read_push_queue(THD* thd, THD* exe_thd, MYSQL* mysql, net_packet_t* enpacket, int net_state)
{
    size_t len;
    net_packet_t* packet;
    ulong field_count;
    uchar *pos;
    int real_len = 0;
    int net_state_in;

    if (enpacket->net_state == NET_STATE_SKIP)
        goto end;
    
    len = mysql_net_read(mysql, &real_len);
    while (len != 0 && len != packet_error)	
    {
        pos=(uchar*) mysql->net.read_pos;
        if (mysql->net.read_pos[0] ==255)
        {
            net_error_enqueue(mysql, thd, exe_thd, len, enpacket);
            break;
        }
        else
        {
            packet = net_queue_get_node(thd, &thd->queue_in, thd->thd_proxy_recv_queue_size, enpacket);
            packet_buffer_str_append(thd, &packet->buffer,
                                     (const char *)mysql->net.read_pos, len);
            PRINT_PACKET(thd, packet, NET_STATE_FULL);
            packet->net_state = NET_STATE_FULL;

            len = mysql_net_read(mysql, &real_len);
        }
    }

    if (len == 0)
    {
        packet = net_queue_get_node(thd, &thd->queue_in, thd->thd_proxy_recv_queue_size, enpacket);
        packet->server_status = mysql->server_status;
        packet->warning_count = mysql->warning_count;
        packet_buffer_str_append(thd, &packet->buffer,
                                 (const char *)mysql->net.read_pos, real_len);
        if (packet->server_status & SERVER_MORE_RESULTS_EXISTS && net_state == NET_STATE_EOF)
            net_state_in = NET_STATE_EOF_NONEND;
        else if (net_state != 0)
            net_state_in = net_state;
        else
            net_state_in = NET_STATE_EOF;

        if ((net_state_in == NET_STATE_OK || net_state_in == NET_STATE_EOF ||
              net_state_in == NET_STATE_FULL_END || 
              net_state_in == NET_STATE_META_EOF || net_state_in == NET_STATE_META_EOF_END) && 
            enpacket->server_status & SERVER_MORE_RESULTS_EXISTS)
            int2store(str_get(&packet->buffer) + 3, 
                mysql->server_status | SERVER_MORE_RESULTS_EXISTS);

        PRINT_PACKET(thd, packet, net_state_in);
        packet->net_state = net_state_in;
    }

    if (len == packet_error)
    {
        packet = net_queue_get_node(thd, &thd->queue_in, thd->thd_proxy_recv_queue_size, enpacket);
        packet->error_code = mysql_errno(mysql);

        my_message(mysql_errno(mysql), mysql_error(mysql), MYF(0));
        strcpy(packet->message, exe_thd->get_stmt_da()->message());
        exe_thd->clear_error();
        PRINT_PACKET(thd, packet, NET_STATE_ERROR);
        packet->net_state = NET_STATE_ERROR;
    }

    net_push_signal(thd, net_state, false);

end:
    return false;
}

int after_proxy_dispatch_query(THD* thd)
{
    int ret = false;

    LEX *lex= thd->lex;
    if (lex->sql_command == SQLCOM_KILL)
    {
        if (lex->kill_type == KILL_TYPE_ID || lex->kill_type == KILL_TYPE_QUERY)
        {
            Item *it= (Item *)lex->value_list.head();
            THD *tmp;
            int id = it->val_int();
            mysql_mutex_lock(&LOCK_thread_count); // For unlink from list
            I_List_iterator<THD> its(threads);
            while ((tmp=its++))
            {
                if (tmp->get_command() == COM_DAEMON)
                    continue;
                if (cmp_backend_thread_id(tmp, id))
                {
                    mysql_mutex_lock(&tmp->LOCK_thd_data);    // Lock from delete
                    break;
                }
            }
            mysql_mutex_unlock(&LOCK_thread_count);
            if (tmp)
            {
                mysql_mutex_unlock(&tmp->LOCK_thd_data);
                proxy_sql_kill(thd, tmp->thread_id, lex->kill_signal, lex->kill_type);
            }
            else
                my_error(1094, MYF(0), id); //Unknown thread id: xxxx
        }
        else
            proxy_sql_kill_user(thd, get_current_user(thd, lex->users_list.head()),
                                lex->kill_signal);
        ret = true;
    }

    return ret;
}

int net_read_pop_queue(THD* thd, MYSQL* mysql, ulonglong* affect_rows)
{
    net_packet_t* packet;
    str_t* buffer;
    net_queue_t* queue = &thd->queue_in;
    int ret = 0;
    int end_flag = 0;
    int reset_diagnostics = true;

    mysql_mutex_lock(&thd->read_lock);
    while (thd->running)	
    {
        if (queue->dequeue_index != queue->enqueue_index)
        {
            packet = &queue->packets[queue->dequeue_index];
            buffer = &packet->buffer;
            if (packet->net_state == NET_STATE_NULL)
            {
                ++thd->cond_wait_count;
                mysql_cond_wait(&thd->read_cond, &thd->read_lock);
                continue;
            }
        }
        else
        {
            ++thd->cond_wait_count;
            mysql_cond_wait(&thd->read_cond, &thd->read_lock);
            continue;
        }

        *affect_rows = packet->ok_info.affected_rows;
        if (packet->net_state == NET_STATE_ERROR)
        {
            thd->get_stmt_da()->reset_diagnostics_area();
            my_message(packet->error_code, packet->message, MYF(0));
            reset_diagnostics = false;
        }

        if (packet->sub_net_state != NET_STATE_GEN)
        {
            switch (packet->net_state)
            {
            case NET_STATE_FULL_END:
            case NET_STATE_OK:
            case NET_STATE_META_EOF_END:
            case NET_STATE_EOF:
            case NET_STATE_EOF_NONEND:
                {
                    my_net_write(&thd->net, (const uchar*)str_get(buffer), 
                        (size_t)str_get_len(buffer));
                    (void) net_flush(&thd->net);
                }
                break;
            case NET_STATE_META_EOF:
                {
                    my_net_write(&thd->net, (const uchar*)str_get(buffer), 
                        (size_t)str_get_len(buffer));
                }
                break;
            case NET_STATE_FULL:
                {
                    my_net_write(&thd->net, (const uchar*)str_get(buffer), 
                        (size_t)str_get_len(buffer));
                    if (packet->command == COM_BINLOG_DUMP)
                        (void) net_flush(&thd->net);
                }
                break;
            case NET_STATE_END_FLUSH:
                (void) net_flush(&thd->net);
            case NET_STATE_EMPTY:
                break;

            default:
                break;
            }
        }
        packet_buffer_str_truncate_0(thd, queue, &packet->buffer);

        /* if the packet is the real client request, the count the net state*/
        if (packet->net_state ==  NET_STATE_ERROR||
            ((packet->net_state == NET_STATE_EOF || 
            packet->net_state == NET_STATE_META_EOF_END || 
            packet->net_state == NET_STATE_EMPTY || 
            packet->net_state == NET_STATE_FULL_END || 
            packet->net_state == NET_STATE_OK || 
            packet->net_state == NET_STATE_END_FLUSH) &&
            packet->sub_net_state != NET_STATE_GEN))
        {
            ret = packet->net_state ==  NET_STATE_ERROR ? true : false;
            if (!(packet->server_status & SERVER_MORE_RESULTS_EXISTS))
                end_flag = true;
        }
        else if (packet->sub_net_state == NET_STATE_GEN)
        {
            /* if the packet is come from proxy, then process it inside */
            if (packet->command == COM_PING || packet->sub_command == SQLCOM_KILL)
            {
                ret = packet->net_state == NET_STATE_ERROR ? true : false;
                end_flag = true;
            }
        }
        
        if (packet->net_state == NET_STATE_ERROR)
            queue->dequeue_index = queue->enqueue_index;
        else
            queue->dequeue_index = (queue->dequeue_index+1) % thd->thd_proxy_recv_queue_size;
        packet->net_state = NET_STATE_NULL;
        if (end_flag)
        {
            if (reset_diagnostics)
            {
                thd->get_stmt_da()->reset_diagnostics_area();
                if (packet->sub_command != SQLCOM_KILL)
                    thd->get_stmt_da()->disable_status();
            }

            mysql_mutex_unlock(&thd->read_lock);
            return ret;
        }
    }
        
    mysql_mutex_unlock(&thd->read_lock);
    return false;
}

backend_conn_t* get_send_conn_from_thd_read(THD* thd, backend_conn_t* conn)
{
    backend_conn_t* send_conn = NULL;
    int i;
    
    for (i = 0; i < thd->read_conn_count; i++)
    {
        send_conn = thd->read_conn[i];
        if (strcasecmp(conn->host, send_conn->host) == 0
            && conn->port == send_conn->port)
            break;
    }

    if (thd->read_conn_count == i)
        return NULL;
    else
        return send_conn;
}

backend_conn_t* get_send_conn_from_thd_write(THD* thd, backend_conn_t* conn)
{
    backend_conn_t* send_conn = NULL;
    int i;
    
    for (i = 0; i < thd->write_conn_count; i++)
    {
        send_conn = thd->write_conn[i];
        if (strcasecmp(conn->host, send_conn->host) == 0
    	    && conn->port == send_conn->port)
    	    break;
    }

    if (thd->write_conn_count == i)
        return NULL;
    else
        return send_conn;
}

int assemble_kill_packet(THD* thd, backend_conn_t* send_conn, str_t* sql_str)
{
    net_packet_t* packet;
    packet = net_queue_get_node(thd, &thd->queue_out, thd->thd_proxy_send_queue_size, NULL);
    packet->send_conn = send_conn;
    packet->command = COM_QUERY;
    packet_buffer_str_append(thd, &packet->buffer,
                             (const char *)str_get(sql_str),
                             str_get_len(sql_str));
    sql_print_information("assemble_kill_packet: %s", str_get(sql_str));
    PRINT_PACKET(thd, packet, NET_STATE_GEN);
    packet->net_state = NET_STATE_GEN;
    packet->sub_command = SQLCOM_KILL;
    mysql_mutex_lock(&thd->executer_lock);
    mysql_cond_signal(&thd->executer_cond);
    mysql_mutex_unlock(&thd->executer_lock);

    ulonglong affect_rows = 0;
    return net_read_pop_queue(thd, (MYSQL*)send_conn->get_mysql(), &affect_rows);
}

int proxy_kill_push_queue_low(
    THD* thd, 
    int count,  
    backend_conn_t** conns, 
    backend_conn_t* last_conn,
    int type,
    str_t* sql_str
)
{
    backend_conn_t* conn;
    int i;
    char thread_id[65] = {0};
    backend_conn_t* send_conn;

    for (i = 0; i < count; i++)
    {
	str_truncate_0(sql_str);
	conn = conns[i];
	if (last_conn == conn && last_conn->get_mysql())
	{
	    MYSQL* mysql = (MYSQL*)last_conn->get_mysql();
	    sprintf(thread_id, "%ld", mysql->thread_id);
	    get_kill_sql(thd, sql_str, true, thread_id);
            if (type == 1)
	        send_conn = get_send_conn_from_thd_read(thd, last_conn);
            else
	        send_conn = get_send_conn_from_thd_write(thd, last_conn);
	    if (send_conn && assemble_kill_packet(thd, send_conn, sql_str) == true)
		return true;
	}
    }
    
    return false;
}

int proxy_kill_push_queue(THD* thd)
{
    int ret = false;
    LEX *lex= thd->lex;
    if (lex->sql_command == SQLCOM_KILL)
    {
        backend_conn_t* conn;
        str_t sql_str;
        int i;

        if (lex->kill_type == KILL_TYPE_ID || lex->kill_type == KILL_TYPE_QUERY)
        {
            Item *it= (Item *)lex->value_list.head();
            THD *tmp;
            int id = it->val_int();
            mysql_mutex_lock(&LOCK_thread_count); // For unlink from list
            I_List_iterator<THD> its(threads);
            while ((tmp=its++))
            {
                if (tmp->get_command() == COM_DAEMON)
                    continue;
                if (tmp->thread_id == id)
                {
                    mysql_mutex_lock(&tmp->LOCK_thd_data);    // Lock from delete
                    break;
                }
            }
            mysql_mutex_unlock(&LOCK_thread_count);
            if (tmp)
            {
                str_init(&sql_str);
                mysql_mutex_unlock(&tmp->LOCK_thd_data);
                if (proxy_kill_push_queue_low(thd, tmp->write_conn_count, 
                        tmp->write_conn, tmp->last_conn, 2, &sql_str) || 
                    proxy_kill_push_queue_low(thd, tmp->read_conn_count, 
                        tmp->read_conn, tmp->last_conn, 1, &sql_str))
                {
                    str_deinit(&sql_str);
                    return true;
                }

                str_deinit(&sql_str);
                my_ok(thd);
            }
            else
            {
                my_error(1094, MYF(0), id); //Unknown thread id: xxxx
            }
        }
        ret = true;
    }
    return ret;
}

int proxy_kill_push_queue_backend(THD* thd)
{
    int ret = false;
    LEX *lex= thd->lex;
    if (lex->sql_command == SQLCOM_KILL)
    {
        backend_conn_t* conn;
        str_t sql_str;
        int i;

        if (lex->kill_type == KILL_TYPE_ID || lex->kill_type == KILL_TYPE_QUERY)
        {
            Item *it= (Item *)lex->value_list.head();
            THD *tmp;
            int id = it->val_int();
            mysql_mutex_lock(&LOCK_thread_count); // For unlink from list
            I_List_iterator<THD> its(threads);
            while ((tmp=its++))
            {
                if (tmp->get_command() == COM_DAEMON)
                    continue;
                if (cmp_backend_thread_id(tmp, id))
                {
                    mysql_mutex_lock(&tmp->LOCK_thd_data);    // Lock from delete
                    break;
                }
            }
            mysql_mutex_unlock(&LOCK_thread_count);
            if (tmp)
            {
                str_init(&sql_str);
                char thread_id[65] = {0};
                mysql_mutex_unlock(&tmp->LOCK_thd_data);
                if (tmp->write_conn_count > 0)
                {
                    for (i = 0; i < tmp->write_conn_count; i++)
                    {
                        str_truncate_0(&sql_str);
                        conn = tmp->write_conn[i];
                        if (conn->conn_inited())
                        {
                            MYSQL* mysql = (MYSQL*)conn->get_mysql();
                            sprintf(thread_id, "%ld", mysql->thread_id);
                            get_kill_sql(thd, &sql_str, true, thread_id);
                            backend_conn_t* send_conn = get_send_conn_from_thd_write(thd, conn);
                            if (assemble_kill_packet(thd, send_conn, &sql_str) == true)
                                return true;
                        }
                    }
                }
                if (tmp->read_conn_count > 0)
                {
                    for (i = 0; i < tmp->read_conn_count; i++)
                    {
                        str_truncate_0(&sql_str);
                        conn = tmp->read_conn[i];
                        if (conn->conn_inited())
                        {
                            MYSQL* mysql = (MYSQL*)conn->get_mysql();
                            sprintf(thread_id, "%ld", mysql->thread_id);
                            get_kill_sql(thd, &sql_str, true, thread_id);
                            backend_conn_t* send_conn = get_send_conn_from_thd_read(thd, conn);
                            if (assemble_kill_packet(thd, send_conn, &sql_str) == true)
                                return true;
                        }
                    }
                }
                str_deinit(&sql_str);
                my_ok(thd);
            }
            else
            {
                my_error(1094, MYF(0), id); //Unknown thread id: xxxx
            }
        }
        //else
        //{
        //    str_init(&sql_str);
        //    if (thd->write_conn_count > 0)
        //    {
        //        for (i = 0; i < thd->write_conn_count; i++)
        //        {
        //            str_truncate_0(&sql_str);
        //            conn = thd->write_conn[i];
        //            get_kill_sql(thd, &sql_str, true, NULL);
        //            assemble_kill_packet(thd, conn, &sql_str);
        //        }
        //    }
        //    if (thd->read_conn_count > 0)
        //    {
        //        for (i = 0; i < thd->read_conn_count; i++)
        //        {
        //            str_truncate_0(&sql_str);
        //            conn = thd->read_conn[i];
        //            get_kill_sql(thd, &sql_str, true, NULL);
        //            assemble_kill_packet(thd, conn, &sql_str);
        //        }
        //    }
        //    str_deinit(&sql_str);
        //}
        ret = true;
    }
    return ret;
}

void proxy_env_option_push_queue(THD* thd, backend_conn_t* conn)
{
    net_packet_t* packet;

    if (conn->env_version < thd->env_version)
    {
        backend_env_t* env_thd = LIST_GET_FIRST(thd->env_list);
        while (env_thd)
        {
            /* it is prossible to existed the invalid env in thd, 
             * if the var is not existed in anywhere, the 
             * str_get_len(&env_thd->str) is 0, then we skip it  */
            if (conn->env_version < env_thd->env_version &&
                str_get_len(&env_thd->str) > 0)
            {
                enum_server_command com_type;
                if (env_thd->type == SQLCOM_CHANGE_DB)
                    com_type = COM_INIT_DB;
                else
                    com_type = COM_QUERY;

                packet = net_queue_get_node(thd, &thd->queue_out, thd->thd_proxy_send_queue_size, NULL);
                packet->send_conn = conn;
                packet->command = com_type;
                packet_buffer_str_append(thd, &packet->buffer,
                                         (const char *)str_get(&env_thd->str),
                                         str_get_len(&env_thd->str));
                PRINT_PACKET(thd, packet, NET_STATE_GEN);
                packet->net_state = NET_STATE_GEN;
                packet->sub_command = env_thd->type;
                // sql_print_information("env enqueue: %d(thd), %d(env %p), %d(conn %p)", 
                //     thd->env_version, env_thd->env_version, env_thd, conn->env_version, conn); 

                mysql_mutex_lock(&thd->executer_lock);
                mysql_cond_signal(&thd->executer_cond);
                mysql_mutex_unlock(&thd->executer_lock);
            }

            env_thd = LIST_GET_NEXT(link, env_thd);
        }
    }

    return;
}

int need_reassemble_sql(THD* thd, net_packet_t* packet, int hash_user)
{
    if (packet->sub_command == SQLCOM_KILL)
    {
        get_kill_sql(thd, &packet->buffer, hash_user, NULL);
        return true;
    }

    if (opt_proxy_reserve_client_address) 
        return false;

    if(packet->sub_command == SQLCOM_GRANT)
    {
        get_grant_sql(thd, &packet->buffer, hash_user);
        return true;
    }
    else if (packet->sub_command == SQLCOM_REVOKE || packet->sub_command == SQLCOM_REVOKE_ALL)
    {
        get_revoke_sql(thd, &packet->buffer, hash_user);
        return true;
    }
    else if (packet->sub_command == SQLCOM_DROP_USER)
    {
        get_drop_user_sql(thd, &packet->buffer, hash_user);
        return true;
    }
    else if (packet->sub_command == SQLCOM_CREATE_USER)
    {
        get_create_user_sql(thd, &packet->buffer, hash_user);
        return true;
    }
    else if (packet->sub_command == SQLCOM_ALTER_USER)
    {
        get_alter_user_sql(thd, &packet->buffer, hash_user);
        return true;
    }
    else if (packet->sub_command == SQLCOM_SHOW_GRANTS)
    {
        get_show_grants_sql(thd, &packet->buffer, hash_user);
        return true;
    }
    else if (packet->sub_command == SQLCOM_RENAME_USER)
    {
        get_rename_user_sql(thd, &packet->buffer, hash_user);
        return true;
    }
    else if (packet->sub_command == SQLCOM_SHOW_CREATE_USER)
    {
        get_show_create_user_sql(thd, &packet->buffer, hash_user);
        return true;
    }

    return false;
}

#if __CONSISTEND_READ__
int proxy_consistend_cache_update_global(THD* thd, backend_conn_t* conn)
{
    TABLE_LIST*     table;

    if (!thd->proxy_enable_consistend_read)
        return false;

    switch (thd->lex->sql_command)
    {
    case SQLCOM_INSERT:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
    case SQLCOM_DELETE:
    case SQLCOM_DELETE_MULTI:
    case SQLCOM_ALTER_TABLE:
    case SQLCOM_TRUNCATE:
        break;

    default:
        return false;
    }

    mysql_mutex_lock(&global_consistend_cache.consistend_lock);
    for (table=thd->lex->query_tables; table; table=table->next_global)
    {
        proxy_update_consistend_cache(&global_consistend_cache, 1,
            table->db, table->table_name, thd->consistend_execute_time);
    }
    mysql_mutex_unlock(&global_consistend_cache.consistend_lock);

    return false;
}

int proxy_extract_update_tables(THD* thd, str_t* query)
{
    TABLE_LIST*     table;
    int first = true;
    int omit = true;
    char tmp[64];

    switch (thd->lex->sql_command)
    {
    case SQLCOM_INSERT:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
    case SQLCOM_DELETE:
    case SQLCOM_DELETE_MULTI:
    case SQLCOM_ALTER_TABLE:
    case SQLCOM_TRUNCATE:
        break;

    default:
        return false;
    }

    str_append(query, "INSERT INTO arkproxy_consistend_read.update_timestamp(instance,"
        "dbname, tablename, updatetime) values");
    for (table=thd->lex->query_tables; table; table=table->next_global)
    {
        if (!first)
            str_append(query, ",");

        str_append(query, "(");
        str_append(query, "1,");
        str_append(query, "'");
        str_append(query, table->db);
        str_append(query, "',");
        str_append(query, "'");
        str_append(query, table->table_name);
        str_append(query, "',");
        sprintf(tmp, "%lld", thd->consistend_execute_time);
        str_append(query, tmp);
        str_append(query, ")");
        first = false;
    }

    str_append(query, "on duplicate key update updatetime=values(updatetime)");
    return false;
}

int proxy_enqueue_consistend_table_write(THD* thd, str_t* query, backend_conn_t* conn)
{
    if (str_get_len(query) == 0)
        return false;

    net_packet_t* packet;
    enum_server_command com_type;
    com_type = COM_QUERY;

    packet = net_queue_get_node(thd, &thd->queue_out, thd->thd_proxy_send_queue_size, NULL);
    packet->send_conn = conn;
    packet->command = com_type;
    packet_buffer_str_append(thd, &packet->buffer, (const char *)str_get(query),
                             str_get_len(query));
    PRINT_PACKET(thd, packet, NET_STATE_GEN);
    packet->net_state = NET_STATE_GEN;
    packet->sub_command = SQLCOM_INSERT;

    mysql_mutex_lock(&thd->executer_lock);
    mysql_cond_signal(&thd->executer_cond);
    mysql_mutex_unlock(&thd->executer_lock);

    return false;
}

int proxy_push_consistend_read_sync(THD* thd, backend_conn_t* conn)
{
    if (!thd->proxy_enable_consistend_read)
        return false;
    str_t query;
    str_init(&query);
    proxy_extract_update_tables(thd, &query);
    proxy_enqueue_consistend_table_write(thd, &query, conn);
    str_deinit(&query);
    return false;
}
#endif

int mysql_push_execute_queue(THD* thd, backend_conn_t* conn)
{
    int ret;
    net_packet_t* packet;

    packet = net_queue_get_node(thd, &thd->queue_out, thd->thd_proxy_send_queue_size, NULL);
    packet->command = thd->get_command();
    packet->send_conn = conn;
    packet->sub_command = thd->lex->sql_command;
    packet->server_status = thd->server_status;

    if (packet->command != COM_INIT_DB && 
        !need_reassemble_sql(thd, packet, thd->lex->route_type != ROUTER_TYPE_NOHASH))
    {
      packet_buffer_str_append(thd, &packet->buffer, (const char *)thd->query(),
                               thd->query_length());
    }

    PRINT_PACKET(thd, packet, NET_STATE_FULL);
    packet->net_state = NET_STATE_FULL;
    conn->running_sql = (char *)thd->query();

    mysql_mutex_lock(&thd->executer_lock);
    mysql_cond_signal(&thd->executer_cond);
    mysql_mutex_unlock(&thd->executer_lock);

#if __CONSISTEND_READ__
    proxy_push_consistend_read_sync(thd, conn);
#endif 

    ulonglong affect_rows = 0;
    int64_t exe_time_1 = my_hrtime().val;
    proxy_set_the_timeout(thd, conn, exe_time_1);
    ret = net_read_pop_queue(thd, (MYSQL*)conn->get_mysql(), &affect_rows);
    proxy_set_the_timeout(thd, NULL, 0);
    int64_t exe_time_2 = my_hrtime().val;
    conn->running_sql = NULL;
    if (!ret)
    {
        set_ark_env(thd, conn);
        if (proxy_digest_on(thd) && thd->get_command() == COM_QUERY)
            proxy_format_to_queue_after(thd, (exe_time_2 - exe_time_1), affect_rows, conn->server->server_name);
#if __CONSISTEND_READ__
        proxy_consistend_cache_update_global(thd, conn);
#endif 
    }

    return ret;
}

void stmtid_cache_dequeue(THD* thd)
{
    stmtid_conn_t* stmtid_conn;

    stmtid_conn = LIST_GET_FIRST(thd->stmtid_cache->stmtid_lst);
    while (stmtid_conn)
    {
        if (thd->stmt_id == stmtid_conn->client_stmt_id)
        {
            sql_print_information("stmt dequeue, client: %d, stmtid: %d", 
                stmtid_conn->client_stmt_id, stmtid_conn->stmt_id);
            LIST_REMOVE(link, thd->stmtid_cache->stmtid_lst, stmtid_conn);
            my_free(stmtid_conn);
            return;
        }
            
        stmtid_conn = LIST_GET_NEXT(link, stmtid_conn);
    }
}

void stmtid_cache_queue(THD* thd, net_packet_t* packet)
{
    stmtid_conn_t* stmtid_conn;
    if (!thd->stmtid_cache)
    {
        thd->stmtid_cache = (stmtid_cache_t*)my_malloc(sizeof(stmtid_cache_t), MY_ZEROFILL);
        LIST_INIT(thd->stmtid_cache->stmtid_lst);
    }

    stmtid_conn = (stmtid_conn_t*)my_malloc(sizeof(stmtid_conn_t), MY_ZEROFILL);
    stmtid_conn->stmt_id = packet->stmt_id;
    stmtid_conn->stmt_conn = packet->send_conn;
    stmtid_conn->client_stmt_id = thd->statement_id_counter & STMT_ID_MASK;

    sql_print_information("stmt enqueue, client: %d, stmtid: %d", 
        stmtid_conn->client_stmt_id, stmtid_conn->stmt_id);
    LIST_ADD_LAST(link, thd->stmtid_cache->stmtid_lst, stmtid_conn);
}

MYSQL* fetch_mysql_or_reconnect(THD *thd, backend_conn_t *conn, int *reconnect,
                              bool *free_mysql) {
  MYSQL* mysql;
  int err = 0;
  int retry_times = 0;
reconnect:
  if (*reconnect)
    err = proxy_reconnect_server(thd, conn);
  if (!err && conn->conn_inited() && conn->get_mysql() != NULL) {
    mysql = (MYSQL *)conn->get_mysql();
    *free_mysql = false;
    *reconnect = false;
  } else {
    retry_times++;
    sql_print_warning(
        "backend connection closed. host:%s, port:%d, reconnect times...:%d ",
        conn->host, conn->port, retry_times);
    if (retry_times <= 10) {
      my_sleep(30 * 1000);
      *reconnect = true;
      goto reconnect;
    } else {
      mysql = mysql_init(NULL);
      proxy_mysql_client_methods(mysql);
      *free_mysql = true;
      *reconnect = true;
    }
  }
  return mysql;
}

void net_read_stmt_prepare(THD* thd, THD *exe_thd, net_packet_t* packet)
{
    str_t* buffer;
    buffer = &packet->buffer;
    backend_conn_t* conn;
    int sql_command;
    int reconnect = false;
    bool free_mysql = false;
    MYSQL* mysql = NULL;
reconnect:
    sql_command = thd->lex->sql_command;
    conn = packet->send_conn;
    if (free_mysql && mysql != NULL) {
        mysql_close(mysql);
        free_mysql = false;
    }
    mysql = fetch_mysql_or_reconnect(thd, conn, &reconnect, &free_mysql);
    //thd->statement_id_counter += 100;
    proxy_send_query(mysql, str_get(buffer), str_get_len(buffer), COM_STMT_PREPARE, 1);
    if (net_read_push_queue_noloop(thd, exe_thd, mysql, packet, NET_STATE_FULL, &reconnect))
        goto reconnect;
    if (packet->param_count)
    {
        if (packet->field_count)
            net_read_push_queue(thd, exe_thd, mysql, packet, NET_STATE_META_EOF);
        net_read_push_queue(thd, exe_thd, mysql, packet, NET_STATE_META_EOF_END);
    }
    else
    {
        if (packet->field_count)
            net_read_push_queue(thd, exe_thd, mysql, packet, NET_STATE_META_EOF_END);
    }
    if (free_mysql) {
        mysql_close(mysql);
    }
    /* cache the stmt id and connection map */
    //stmtid_cache_queue(thd, packet);
}

void net_read_stmt_execute(THD* thd, THD *exe_thd, net_packet_t* packet)
{
    str_t* buffer;
    buffer = &packet->buffer;
    backend_conn_t* conn;
    int sql_command;
    int reconnect = false;
    bool free_mysql = false;
    MYSQL* mysql;
reconnect:
    sql_command = thd->lex->sql_command;
    conn = packet->send_conn;
    if (free_mysql && mysql != NULL) {
        mysql_close(mysql);
        free_mysql = false;
    }
    mysql = fetch_mysql_or_reconnect(thd, conn, &reconnect, &free_mysql);
    proxy_send_query(mysql, str_get(buffer), str_get_len(buffer), COM_STMT_EXECUTE, 1);
    /* packet header */
    if(net_read_push_queue_noloop(thd, exe_thd, mysql, packet, NET_STATE_FULL, &reconnect))
        goto reconnect;
    if (packet->field_count)
    {
        /* metadata */
        net_read_push_queue(thd, exe_thd, mysql, packet, NET_STATE_META_EOF);

        /* rows results */
        if (mysql->server_status & SERVER_STATUS_CURSOR_EXISTS)
        {
            net_noread_push_queue(thd, exe_thd, mysql, packet, NET_STATE_END_FLUSH);
        }
        else
        {
            net_read_push_queue(thd, exe_thd, mysql, packet, NET_STATE_META_EOF_END);
        }
    }
    if (free_mysql) {
        mysql_close(mysql);
    }
}

void
send_and_read_by_com_type(THD* thd, THD *exe_thd, net_packet_t* packet)
{
    str_t* buffer;
    buffer = &packet->buffer;
    backend_conn_t* conn;
    int reconnect = false;

    conn = packet->send_conn;

    bool free_mysql = false;
    MYSQL* mysql;
reconnect:
    if (free_mysql && mysql != NULL) {
        mysql_close(mysql);
        free_mysql = false;
    }
    mysql = fetch_mysql_or_reconnect(thd, conn, &reconnect, &free_mysql);

    switch (packet->command) {
    case COM_QUERY:
        proxy_send_query(mysql, str_get(buffer), str_get_len(buffer), COM_QUERY, 1);
        do {
            if (net_read_push_queue_noloop(thd, exe_thd, mysql, 
                  packet, NET_STATE_FULL, &reconnect))
                goto reconnect;

            if (packet->field_count)
            {
                /* metadata */
                net_read_push_queue(thd, exe_thd, mysql, packet, NET_STATE_META_EOF);

                /* rows results */
                if (mysql->server_status & SERVER_STATUS_CURSOR_EXISTS)
                {
                    net_noread_push_queue(thd, exe_thd, mysql, 
                        packet, NET_STATE_END_FLUSH);
                }
                else
                {
                    net_read_push_queue(thd, exe_thd, mysql, 
                        packet, NET_STATE_META_EOF_END);
                }
            }
        } while ((mysql)->server_status & SERVER_MORE_RESULTS_EXISTS);
        break;
    case COM_INIT_DB:
        proxy_send_query(mysql, thd->db, thd->db_length, COM_INIT_DB, 1);
        my_free(mysql->db);
        mysql->db = my_strdup(thd->db,MYF(MY_WME));
        if (net_read_push_queue_noloop(thd, exe_thd, mysql, packet, NET_STATE_OK, &reconnect))
            goto reconnect;
        break;
    case COM_STMT_PREPARE:
        net_read_stmt_prepare(thd, exe_thd, packet);
        break;
    case COM_STMT_EXECUTE:
        net_read_stmt_execute(thd, exe_thd, packet);
        break;
    case COM_STMT_SEND_LONG_DATA:
        proxy_send_query(mysql, str_get(buffer), 
            str_get_len(buffer), COM_STMT_SEND_LONG_DATA, 1);
        net_noread_push_queue(thd, exe_thd, mysql, packet, NET_STATE_EMPTY);
        break;
    case COM_STMT_CLOSE:
        proxy_send_query(mysql, str_get(buffer), 
            str_get_len(buffer), COM_STMT_CLOSE, 1);
        net_noread_push_queue(thd, exe_thd, mysql, packet, NET_STATE_EMPTY);
        //stmtid_cache_dequeue(thd);
        break;
    case COM_STMT_RESET:
        proxy_send_query(mysql, str_get(buffer), 
            str_get_len(buffer), COM_STMT_RESET, 1);
        if (net_read_push_queue_noloop(thd, exe_thd, mysql, packet, NET_STATE_OK, &reconnect))
            goto reconnect;
        break;
    case COM_REGISTER_SLAVE:
    case COM_REFRESH:
    case COM_PROCESS_KILL:
        proxy_send_query(mysql, str_get(buffer), 
            str_get_len(buffer), packet->command, 1);
        if (net_read_push_queue_noloop(thd, exe_thd, mysql, packet, NET_STATE_OK, &reconnect))
            goto reconnect;
        break;
    case COM_RESET_CONNECTION:
        proxy_send_query(mysql, 0, 0, packet->command, 1);
        if (net_read_push_queue_noloop(thd, exe_thd, mysql, packet, NET_STATE_OK, &reconnect))
            goto reconnect;
        break;
    case COM_FIELD_LIST:
    case COM_SET_OPTION:
    case COM_BINLOG_DUMP:
    case COM_STMT_FETCH:
        proxy_send_query(mysql, str_get(buffer), 
            str_get_len(buffer), packet->command, 1);
        net_read_push_queue(thd, exe_thd, mysql, packet, NET_STATE_EOF);
        break;
    case COM_PROCESS_INFO:
    case COM_DEBUG:
        proxy_send_query(mysql, 0, 0, packet->command, 1);
        net_read_push_queue(thd, exe_thd, mysql, packet, 0);
        break;
    case COM_STATISTICS:
        proxy_send_query(mysql, 0, 0, packet->command, 1);
        if (net_read_push_queue_noloop(thd, exe_thd, mysql, packet, 
              NET_STATE_FULL_END, &reconnect))
            goto reconnect;
        break;
    case COM_PING:
        proxy_send_query(mysql, 0, 0, COM_PING, 1);
        if (net_read_push_queue_noloop(thd, exe_thd, mysql, packet, NET_STATE_OK, &reconnect))
            goto reconnect;
        break;
    default:
        break;
    }

    if (free_mysql) {
        mysql_close(mysql);
    }
    //if (packet->command != COM_PING)
    //    conn->last_execute = my_hrtime().val;
    return;
}

int proxy_heartbeat_queue_low(THD* thd, THD* exe_thd, int count, backend_conn_t** conns)
{
    int i;
    net_packet_t packet;
    backend_conn_t* conn;
    int retry_count = 0;

    for (i = 0; i < count; i++)
    {
        retry_count = 0;
        conn = conns[i];
        ulonglong currtime = my_hrtime().val;
        /* if connection is valid and the time since last heartbeat is 
         * more then half of wait_timeout, do heartbeat again */
        /* if the connection is invalid, then skip it */
        if (!conn || conn->lazy_conn_needed || !conn->conn_inited())
            continue;
        if (conn->wait_timeout * 500000 < currtime - conn->last_heartbeat ||
            proxy_server_heartbeat_period * 1000000 < currtime - conn->last_heartbeat)
        {
            // sql_print_information("PING task enqueue");
            // packet = net_queue_get_node(thd, &thd->queue_out, proxy_send_queue_size, NULL);
            /* enqueue must been serializable, so here can not to use the 
             * queue, we should use local variables to ping */
retry:
            packet.send_conn = conn;
            packet.command = COM_PING;
            str_init(&packet.buffer);

            PRINT_PACKET(thd, &packet, NET_STATE_GEN);
            packet.net_state = NET_STATE_GEN;

            send_and_read_by_com_type(thd, exe_thd, &packet);
            //net_push_signal(thd, 0, true);
            if (packet.net_state == NET_STATE_ERROR)
            {
                retry_count ++;
                sql_print_warning("proxy heartbeat error(retry: %d): %d, %s", 
                    retry_count, packet.error_code, packet.message);
                if (retry_count >= 3) {
                    conn->inited = false;
                    close_backend_conn(conn);
                }

                else
                    goto retry;
            }
            else
            {
                conn->last_heartbeat = my_hrtime().val;
            }
        }
    }

    return false;
}

int proxy_heartbeat_queue(THD* thd, THD* exe_thd)
{
    proxy_heartbeat_queue_low(thd, exe_thd, thd->write_conn_count, thd->write_conn);
    proxy_heartbeat_queue_low(thd, exe_thd, thd->read_conn_count, thd->read_conn);
    return false;
}

int proxy_timer_checker(THD* thd)
{
    backend_conn_t* conn;
    int i;

    ulonglong currtime;

    if (my_hrtime().val - thd->last_update_slave_time > 
        proxy_check_slave_lag_period * 1000000 ||
        thd->last_update_slave_time == 0)
    {
        for (i = 0; i < thd->read_conn_count; i++)
        {
            conn = thd->read_conn[i];
            if (!conn || !conn->conn_inited())
                continue;
            mysql_fetch_server_status(conn);
        }

        thd->last_update_slave_time = my_hrtime().val;
    }

#if __CONSISTEND_READ__
    if (my_hrtime().val - thd->last_update_consistend_cache_time > 
        proxy_check_consistend_period * 1000000 ||
        thd->last_update_consistend_cache_time == 0)
    {
        for (i = 0; i < thd->read_conn_count; i++)
        {
            conn = thd->read_conn[i];
            if (!conn || !conn->conn_inited() || !conn->consistend_cache)
                continue;
            mysql_fetch_slave_consistend_read_table(conn);
        }

        thd->last_update_consistend_cache_time = my_hrtime().val;
    }
#endif

    return false;
}

pthread_handler_t command_executer(void* arg)
{
    THD *thd= NULL;
    THD *exe_thd= NULL;
    int error;

    my_thread_init();
    exe_thd = new THD(0);
    exe_thd->thread_stack= (char*) &exe_thd;

    thd = (THD*)arg;
    net_queue_t* queue = &thd->queue_out;
    net_packet_t* packet;
    setup_connection_thread_globals(exe_thd);
    struct timespec abstime;
    thd->thread_running = true;

    mysql_mutex_lock(&thd->executer_lock);
    while (thd->running)
    {
        proxy_timer_checker(thd);
        
        if (queue->dequeue_index != queue->enqueue_index)
        {
            packet = &queue->packets[queue->dequeue_index];
            if (packet->net_state == NET_STATE_NULL)
            {
                /* wait 10s, and then wake to heartbeat, the heartbeat delay 10s */
                set_timespec_nsec(abstime, 10000000000LL);
                mysql_cond_timedwait(&thd->executer_cond, &thd->executer_lock, &abstime);
                proxy_heartbeat_queue(thd, exe_thd);
                continue;
            }
        }
        else
        {
            /* wait 10s, and then wake to heartbeat, the heartbeat delay 10s */
            set_timespec_nsec(abstime, 10000000000LL);
            mysql_cond_timedwait(&thd->executer_cond, &thd->executer_lock, &abstime);
            proxy_heartbeat_queue(thd, exe_thd);
            continue;
        }

        /* here, it is no need to process the connection inited, 
         * it should been processed in the router function, here 
         * only report the lost connection to client(or proxy )*/
        // sql_print_information("execute task");
        send_and_read_by_com_type(thd, exe_thd, packet);
        //net_push_signal(thd, 0, true);
        /* check the env settings */

        packet->net_state = NET_STATE_NULL;
        queue->dequeue_index = (queue->dequeue_index+1) % thd->thd_proxy_send_queue_size;
    }

    mysql_mutex_unlock(&thd->executer_lock);
    thd->thread_running = false;
    mysql_mutex_lock(&LOCK_thread_count);
    exe_thd->unlink();
    mysql_mutex_unlock(&LOCK_thread_count);
    delete exe_thd;
    my_thread_end();
    pthread_exit(0);
    return NULL;
}

int mysql_deinit_net_queue(THD* thd)
{
    net_packet_t* packet;
    if (thd == NULL || !thd->running)
        return TRUE;

    if (thd->connection_type == PROXY_CONNECT_SHELL)
        return false;

    thd->running = false;

    net_queue_t* queue = &thd->queue_in;
    net_queue_t* outqueue = &thd->queue_out;

    /* wait thread to exit */
    while (thd->thread_running)
    {
        mysql_cond_signal(&thd->executer_cond);
        mysql_cond_signal(&thd->read_cond);
        sleep(0);
    }

    mysql_mutex_destroy(&thd->executer_lock);
    mysql_cond_destroy(&thd->executer_cond);
    mysql_mutex_destroy(&thd->read_lock);
    mysql_cond_destroy(&thd->read_cond);

    for (ulong i=0; i < thd->thd_proxy_send_queue_size; i ++)
    {
        packet = &outqueue->packets[i];
        str_deinit(&packet->buffer);
    }
  
    for (ulong i=0; i < thd->thd_proxy_recv_queue_size; i ++)
    {
        packet = &queue->packets[i];
        str_deinit(&packet->buffer);
    }

    my_free(outqueue->packets);
    my_free(queue->packets);

    proxy_release_thd_timeout(thd);
    __reset_rules(thd->query_rule->_thr_SQP_rules);
    if (thd->query_rule->_thr_SQP_rules)
        delete thd->query_rule->_thr_SQP_rules;
    my_free(thd->query_rule);
    thd->query_rule = NULL;
    return false;
}

int mysql_init_net_queue(THD* thd)
{
    net_queue_t* queue = &thd->queue_in;
    net_queue_t* outqueue = &thd->queue_out;
    net_packet_t* packet;
  
    mysql_mutex_init(NULL, &thd->executer_lock, MY_MUTEX_INIT_FAST);
    mysql_cond_init(NULL, &thd->executer_cond, NULL);
    mysql_mutex_init(NULL, &thd->read_lock, MY_MUTEX_INIT_FAST);
    mysql_cond_init(NULL, &thd->read_cond, NULL);
    /* 发送队列初始化 */
    thd->thd_proxy_send_queue_size = proxy_send_queue_size;
    thd->queue_in_packet_total_len = NAME_CHAR_LEN * proxy_recv_queue_size;
    thd->queue_out_packet_total_len = NAME_CHAR_LEN * proxy_send_queue_size;
    thd->queue_packet_buf_size =
        NAME_CHAR_LEN * (proxy_send_queue_size + proxy_recv_queue_size);
    outqueue->packets = (net_packet_t*)my_malloc(
        thd->thd_proxy_send_queue_size * sizeof(net_packet_t), MYF(0));
    for (ulong i=0; i < thd->thd_proxy_send_queue_size; i ++)
    {
        packet = &outqueue->packets[i];
        packet->net_state = NET_STATE_NULL;
        str_init(&packet->buffer);
    }
  
    /* 接收队列初始化 */
    thd->thd_proxy_recv_queue_size = proxy_recv_queue_size;
    queue->packets = (net_packet_t*)my_malloc(
        thd->thd_proxy_recv_queue_size * sizeof(net_packet_t), MYF(0));
    for (ulong i=0; i < thd->thd_proxy_recv_queue_size; i ++)
    {
        packet = &queue->packets[i];
        packet->net_state = NET_STATE_NULL;
        str_init(&packet->buffer);
    }
    queue->dequeue_index = 0;
    queue->enqueue_index = 0;
    outqueue->dequeue_index = 0;
    outqueue->enqueue_index = 0;
  
    //memset(thd->write_conn, 0, sizeof(backend_conn_t*) * MAX_CLUSTER_NODES);
    //memset(thd->read_conn, 0, sizeof(backend_conn_t*) * MAX_CLUSTER_NODES);
    thd->write_conn_count = 0;
    thd->read_conn_count = 0;
    thd->first_request = true;
    global_proxy_config.config_read_lock();
    load_balance_for_server(thd);
    global_proxy_config.config_unlock();

    thd->query_rule = (query_rules_t*)my_malloc(sizeof(query_rules_t), MYF(0));
    thd->query_rule->qpo = NULL;
    thd->query_rule->digest_text = NULL;
    thd->query_rule->rule_version = 0;
    thd->query_rule->timeout_thd = NULL;
    thd->query_rule->_thr_SQP_rules = NULL;
    proxy_assign_thd_timeout(thd);
    //thd->config_version = global_proxy_config.config_version;
    
#if __CONSISTEND_READ__
    thd->proxy_enable_consistend_read = proxy_enable_consistend_read;
#endif

    thd->running = true;
    thd->trace_queue = NULL;
  
    return false;
}

int create_command_executor_thread(THD* thd) 
{
    pthread_t thread_id;
    /**/
    int error;
    if ((error = mysql_thread_create(0, &thread_id, &connection_attrib, command_executer, (void*)thd)))
    {
        my_error(ER_CANT_CREATE_THREAD, MYF(ME_FATALERROR), error);
        sql_print_warning("arkproxy create command executor thread failed(%d)", error);
        return true;
    }

    //pthread_detach(thread_id);
    return false;
}

int load_balance_for_server(THD *thd)
{
    proxy_servers_t *best = NULL;
    proxy_servers_t* server = LIST_GET_FIRST(global_proxy_config.server_lst);
    while (server)
    {
        if (best == NULL || (best->current_weight < server->current_weight &&
            server->server_status == SERVER_STATUS_ONLINE))
            best = server;
        server = LIST_GET_NEXT(link, server);
    }
    if (best)
    {
        thd->first_request_best_server = best;
    }
    return false;
}

