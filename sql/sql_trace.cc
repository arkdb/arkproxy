#include "sql_class.h"
#include "sql_connect.h"
#include "sql_trace.h"
#include "password.h"
#include "mysql/psi/mysql_thread.h"
#include <mysql.h>
#include "rdkafka.h"
#include "ark_config.h"

extern int mysql_dup_char(char* src, char* dest, char chr);
extern int mysql_dup_char_with_len(char* src, char* dest, char chr, int src_len);
extern MYSQL* proxy_get_trace_server_connection_with_con(backend_conn_t* conn);
extern int deinit_format_cache_node(format_cache_node_t* format_cache_node, int free_all);
extern MYSQL* proxy_get_config_server_connection_with_con(backend_conn_t* conn);
extern bool close_backend_conn(backend_conn_t *conn);

pthread_handler_t proxy_trace_format_thread(void* arg);
pthread_handler_t proxy_trace_flush_thread(void* arg);
pthread_handler_t proxy_sql_bucket_flush_thread(void* arg);
int proxy_create_digest_table();
int proxy_create_sql_table();

trace_cache_t global_trace_cache_space;
trace_cache_t* global_trace_cache = &global_trace_cache_space;
int trace_digest_inited = false;
int trace_sql_inited = false;
int trace_inited = false;

bool proxy_busy_wait()
{
    if (proxy_trace_busy_strategy == BUSY_WAIT)
    {
        my_sleep(proxy_trace_busy_wait_time_msec);
        return true;
    }
    else if (proxy_trace_busy_strategy == BUSY_SKIP)
    {
        return false;
    }
    return false;
}

uint64_t 
proxy_get_digest(
    format_cache_node_t* trace_node
)
{
    str_t str;
    uint64_t digest;

    str_init(&str);
    str_append(&str, trace_node->username);
    //str_append(&str, trace_node->hostname);
    str_append(&str, trace_node->dbname);
    str_append_with_length(&str, str_get(trace_node->format_sql), 
        str_get_len(trace_node->format_sql));
    digest = md5_16_int(str_get(&str));
    str_deinit(&str);
    return digest;
}

uchar *proxy_trace_get_key(
    format_cache_node_t* trace_node, 
    size_t *length,
    my_bool not_used __attribute__((unused)))
{
    if (trace_node->hash_key[0] == '\0')
    {
        uint64_t digest;
        digest = proxy_get_digest(trace_node);
        sprintf(trace_node->hash_key, "0x%016llX", (long long unsigned int)digest);
        //my_make_scrambled_password(trace_node->hash_key, str_get(&str), str_get_len(&str));
    }

    *length= strlen(trace_node->hash_key);
    return (uchar*)trace_node->hash_key;
}

uchar *proxy_sql_get_key(
    char* sql_key, 
    char* sql_statements, 
    int sql_length, 
    size_t *length,
    my_bool not_used __attribute__((unused)))
{
    uint64_t digest;
    digest = md5_16_int(sql_statements);
    sprintf(sql_key, "0x%016llX", (long long unsigned int)digest);
    //my_make_scrambled_password(sql_key, sql_statements, sql_length);
    *length= strlen(sql_key);
    return (uchar*)sql_key;
}

int proxy_trace_sql_on(int logged)
{
    /* use the variable proxy_sql_trace to control the trace
     * record, but the global_trace_cache is inited now prossible,
     * we can free the trace_digest and trace_sql using the arkproxy shell command, 
     * and if the proxy_sql_trace is no, but trace_sql_inited is off, then the 
     * trace is not avialable still, we not to record, 
     * and trace_sql is depend on the digest cache, so also need to proxy_digest_on is on*/
    return ((proxy_sql_trace || logged) && 
        trace_sql_inited && proxy_digest_cache_on());
}

int proxy_trace_version()
{
    return global_trace_cache->trace_version;
}

int proxy_digest_cache_on()
{
    /* use the variable proxy_digest_trace to control the trace
     * record, but the global_trace_cache is inited now prossible,
     * we can free the trace_digest and trace_sql using the arkproxy shell command, 
     * and if the proxy_digest_trace is no, but trace_digest_inited is off, then the 
     * trace is not avialable still, we not to record*/
    return trace_digest_inited;
}

int proxy_digest_on(THD* thd)
{
    if (thd->connection_type != PROXY_CONNECT_PROXY)
        return false;

    /* use the variable proxy_digest_trace to control the trace
     * record, but the global_trace_cache is inited now prossible,
     * we can free the trace_digest and trace_sql using the arkproxy shell command, 
     * and if the proxy_digest_trace is no, but trace_digest_inited is off, then the 
     * trace is not avialable still, we not to record*/
    return ((thd->trace_digest_on || 
            (thd->query_rule && thd->query_rule->qpo && thd->query_rule->qpo->log > 0)) && 
            trace_digest_inited);
}

static void dr_msg_cb(
    rd_kafka_t *rk,
    const rd_kafka_message_t *rkmessage, 
    void *opaque
)
{
    if(rkmessage->err)
        sql_print_error("kafka message send error. %s", rd_kafka_err2str(rkmessage->err));
    //else
        //sql_print_information("kafka send (%zd bytes, " "partition %"PRId32")\n",
        //            rkmessage->len, rkmessage->partition);
    /* rkmessage被librdkafka自动销毁*/
}

int proxy_kafka_produce(rd_kafka_topic_t* rkt, rd_kafka_t* rk, str_t* digest)
{
    int retry_count = 0;
    int err = 0;
retry:
     /*Send/Produce message.
       这是一个异步调用，在成功的情况下，只会将消息排入内部producer队列，
       对broker的实际传递尝试由后台线程处理，之前注册的传递回调函数(dr_msg_cb)
       用于在消息传递成功或失败时向应用程序发回信号*/
    if (rd_kafka_produce(
        /* Topic object */
        rkt,
        /*使用内置的分区来选择分区*/
        RD_KAFKA_PARTITION_UA,
        /*生成payload的副本*/
        RD_KAFKA_MSG_F_COPY,
        /*消息体和长度*/
        str_get(digest), str_get_len(digest),
        /*可选键及其长度*/
        NULL, 0, &err) == -1)
    {
        sql_print_error("proxy failed to produce to topic %s: %s\n",
            rd_kafka_topic_name(rkt),
            rd_kafka_err2str(rd_kafka_last_error()));

        if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL &&
            retry_count ++ <= 1)
        {
            /*如果内部队列满，等待消息传输完成并retry,
            内部队列表示要发送的消息和已发送或失败的消息，
            内部队列受限于queue.buffering.max.messages配置项*/
            rd_kafka_poll(rk, 1);
            goto retry;
        }
    }
    else
    {
        //sql_print_information("proxy enqueued message (%zd bytes) for topic %s\n",
        //    (ulong)str_get_len(digest), rd_kafka_topic_name(rkt));
    }

    /*producer应用程序应不断地通过以频繁的间隔调用rd_kafka_poll()来为
    传送报告队列提供服务。在没有生成消息以确定先前生成的消息已发送了其
    发送报告回调函数(和其他注册过的回调函数)期间，要确保rd_kafka_poll()
    仍然被调用*/
    rd_kafka_poll(rk, 0);

    return false;
}

int proxy_trace_kafka_create(
    char* topic,
    rd_kafka_topic_t** rkt_ret,
    rd_kafka_t** rk_ret
)
{
    rd_kafka_t *rk = NULL;            /*Producer instance handle*/
    rd_kafka_topic_t *rkt = NULL;     /*topic对象*/
    rd_kafka_conf_t *conf;            /*临时配置对象*/
    char hostport[256];
    char errstr[512];

    *rkt_ret = rkt;
    *rk_ret = rk;
   /* 创建一个kafka配置占位 */
    conf = rd_kafka_conf_new();
 
    if (proxy_trace_storage_host && proxy_trace_storage_port)
    {
        sprintf(hostport, "%s:%ld", proxy_trace_storage_host, proxy_trace_storage_port);
    }
    else
    {
        sql_print_warning("Trace kafka host/port is null, close the trace system");
        return false;
    }

    /*创建broker集群*/
    if (proxy_trace_storage_user && proxy_trace_storage_passwd)
    {
        if (rd_kafka_conf_set(conf, "sasl.password", 
              proxy_trace_storage_passwd, errstr, sizeof(errstr)) ||
          rd_kafka_conf_set(conf, "sasl.username", 
              proxy_trace_storage_user, errstr, sizeof(errstr)))
        {
            my_error(ER_NO_BACKEND_FOUND, MYF(0), errstr);
            sql_print_error("Trace kafka create error. %s", errstr);
            return true;
        }
    }

    if (rd_kafka_conf_set(conf, "bootstrap.servers", 
          hostport, errstr, sizeof(errstr)) ||
        rd_kafka_conf_set(conf, "request.required.acks", 
          "0", errstr, sizeof(errstr)) ||
        rd_kafka_conf_set(conf, "request.timeout.ms", 
          "1", errstr, sizeof(errstr)) ||
        rd_kafka_conf_set(conf, "queue.buffering.max.messages", 
          "100000", errstr, sizeof(errstr)) ||
        rd_kafka_conf_set(conf, "compression.codec", 
          "gzip", errstr, sizeof(errstr)))
    {
        my_error(ER_NO_BACKEND_FOUND, MYF(0), errstr);
        sql_print_error("Trace kafka create error. %s", errstr);
        return true;
    }
 
    /*设置发送报告回调函数，rd_kafka_produce()接收的每条消息都会调用一次该回调函数
     *应用程序需要定期调用rd_kafka_poll()来服务排队的发送报告回调函数*/
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
 
    /*创建producer实例
      rd_kafka_new()获取conf对象的所有权,应用程序在此调用之后不得再次引用它*/
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if(!rk)
    {
        my_error(ER_NO_BACKEND_FOUND, MYF(0), errstr);
        sql_print_error("Trace kafka create producer error: %s", errstr);
        return true;
    }
 
    /*实例化一个或多个topics(`rd_kafka_topic_t`)来提供生产或消费，topic
    对象保存topic特定的配置，并在内部填充所有可用分区和leader brokers，*/
    rkt = rd_kafka_topic_new(rk, topic, NULL);
    if (!rkt){
        my_error(ER_NO_BACKEND_FOUND, MYF(0), errstr);
        sql_print_error("Trace kafka create topic error: %s", errstr);
        rd_kafka_destroy(rk);
        return true;
    }

    *rkt_ret = rkt;
    *rk_ret = rk;
    return false;
}

int proxy_sql_trace_deinit()
{
    mysql_mutex_lock(&global_trace_cache->trace_lock);
    if (!trace_sql_inited)
    {
        mysql_mutex_unlock(&global_trace_cache->trace_lock);
        return false;
    }

    trace_sql_inited = false;
    for (int i=0; i < global_trace_cache->trace_sql_bucket_size; ++i)
    {
        for (int k=0; k < global_trace_cache->trace_sql_bucket[i].bucket_size; ++k)
        {
            mysql_mutex_destroy(&global_trace_cache->trace_sql_bucket[i].sql_bucket[k].trace_lock);
            str_deinit(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].sql_statements);
            my_free(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].sql_statements);
            str_deinit(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].format_sql);
            my_free(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].format_sql);
            str_deinit(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].errmsg);
            my_free(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].errmsg);
        }

        while (global_trace_cache->trace_sql_bucket[i].running)
        {
            mysql_mutex_lock(&global_trace_cache->trace_sql_bucket[i].bucket_lock);
            mysql_cond_signal(&global_trace_cache->trace_sql_bucket[i].bucket_cond);
            mysql_mutex_unlock(&global_trace_cache->trace_sql_bucket[i].bucket_lock);
        }

        mysql_mutex_destroy(&global_trace_cache->trace_sql_bucket[i].bucket_lock);
        mysql_cond_destroy(&global_trace_cache->trace_sql_bucket[i].bucket_cond);
        my_free(global_trace_cache->trace_sql_bucket[i].sql_bucket);
    }
    if (global_trace_cache->trace_storage_type != PROXY_TRACE_STORAGE_MYSQL)
    {
        rd_kafka_topic_destroy((rd_kafka_topic_t*)global_trace_cache->trace_sql_rkt);
        rd_kafka_destroy((rd_kafka_t*)global_trace_cache->trace_sql_rk);
    }
    my_free(global_trace_cache->trace_sql_bucket);
    mysql_mutex_unlock(&global_trace_cache->trace_lock);
    return false;
}

int proxy_sql_trace_check_empty()
{
    mysql_mutex_lock(&global_trace_cache->trace_lock);
    if (!trace_sql_inited)
    {
        mysql_mutex_unlock(&global_trace_cache->trace_lock);
        return true;
    }

    for (int i=0; i < global_trace_cache->trace_sql_bucket_size; ++i)
    {
        if (global_trace_cache->trace_sql_bucket[i].bucket_dequeue_index != 
            global_trace_cache->trace_sql_bucket[i].bucket_enqueue_index)
        {
            mysql_mutex_unlock(&global_trace_cache->trace_lock);
            return false;
        }
    }

    mysql_mutex_unlock(&global_trace_cache->trace_lock);
    return true;
}

int proxy_sql_trace_init()
{
    pthread_t threadid;
    mysql_mutex_lock(&global_trace_cache->trace_lock);
    if (trace_sql_inited)
    {
        mysql_mutex_unlock(&global_trace_cache->trace_lock);
        return false;
    }

    trace_sql_inited = true;
    global_trace_cache->local_proxy_sql_trace = proxy_sql_trace;
    global_trace_cache->trace_sql_bucket_size = proxy_sql_trace_thread_num;

    if (global_trace_cache->trace_storage_type != PROXY_TRACE_STORAGE_MYSQL)
    {
        if (proxy_trace_kafka_create("trace_sql", 
            (rd_kafka_topic_t**)&global_trace_cache->trace_sql_rkt,
            (rd_kafka_t**)&global_trace_cache->trace_sql_rk))
        {
            //mysql_mutex_unlock(&global_trace_cache->trace_lock);
            sql_print_error("proxy create trace kafka error: %s", 
                current_thd->get_stmt_da()->message());
            //return true;
        }
    }

    global_trace_cache->trace_sql_bucket = 
      (trace_sql_bucket_t*)my_malloc(sizeof(trace_sql_bucket_t) *
        global_trace_cache->trace_sql_bucket_size, MY_ZEROFILL);

    for (int i=0; i < global_trace_cache->trace_sql_bucket_size; ++i)
    {
        mysql_mutex_init(NULL, &global_trace_cache->trace_sql_bucket[i].bucket_lock, 
            MY_MUTEX_INIT_FAST);
        mysql_cond_init(NULL, &global_trace_cache->trace_sql_bucket[i].bucket_cond, NULL);
        global_trace_cache->trace_sql_bucket[i].bucket_enqueue_index = 0;
        global_trace_cache->trace_sql_bucket[i].bucket_dequeue_index = 0;
        global_trace_cache->trace_sql_bucket[i].bucket_size = proxy_trace_sql_bucket_length;
        global_trace_cache->trace_sql_bucket[i].current_length = 0;
        global_trace_cache->trace_sql_bucket[i].sql_bucket =
            (format_cache_node_t*)my_malloc(sizeof(format_cache_node_t)*
            global_trace_cache->trace_sql_bucket[i].bucket_size, MY_ZEROFILL);
        for (int k=0; k < global_trace_cache->trace_sql_bucket[i].bucket_size; ++k)
        {
            mysql_mutex_init(NULL,
                             &global_trace_cache->trace_sql_bucket[i].sql_bucket[k].trace_lock,
                             MY_MUTEX_INIT_FAST);
            LIST_INIT(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].rt_lst);
            global_trace_cache->trace_sql_bucket[i].sql_bucket[k].state = TRACE_NODE_NULL;
            global_trace_cache->trace_sql_bucket[i].sql_bucket[k].sql_statements =
                            (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
            str_init(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].sql_statements);
            global_trace_cache->trace_sql_bucket[i].sql_bucket[k].format_sql=
                            (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
            str_init(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].format_sql);
            global_trace_cache->trace_sql_bucket[i].sql_bucket[k].errmsg =
                            (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
            str_init(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].errmsg);
            global_trace_cache->trace_sql_bucket[i].sql_bucket[k].errflag = 0;
            memset(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].hostname, 
                0, HOSTNAME_LENGTH + 1);
            memset(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].dbname, 
                0, NAME_CHAR_LEN + 1);
            memset(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].username, 
                0, NAME_CHAR_LEN + 1);
            memset(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].client_ip, 0, 64);
            memset(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].hash_key, 0, 64);
            memset(global_trace_cache->trace_sql_bucket[i].sql_bucket[k].sql_key, 0, 64);
            global_trace_cache->trace_sql_bucket[i].sql_bucket[k].count = 0;
            global_trace_cache->trace_sql_bucket[i].sql_bucket[k].last_seen = 0;
            global_trace_cache->trace_sql_bucket[i].sql_bucket[k].first_seen = 0;
            global_trace_cache->trace_sql_bucket[i].sql_bucket[k].sum_time = 0;
            global_trace_cache->trace_sql_bucket[i].sql_bucket[k].min_time = 0;
            global_trace_cache->trace_sql_bucket[i].sql_bucket[k].max_time = 0;
            global_trace_cache->trace_sql_bucket[i].sql_bucket[k].affect_rows = 0;
        }
        mysql_thread_create(0, &threadid, &connection_attrib,
            proxy_sql_bucket_flush_thread,
            (void*)&global_trace_cache->trace_sql_bucket[i]);
    }

    mysql_mutex_unlock(&global_trace_cache->trace_lock);
    return false;
}

/* called when start the arkproxy server */
int proxy_trace_cache_create()
{
    if (!trace_inited)
    {
        LIST_INIT(global_trace_cache->free_list);
        LIST_INIT(global_trace_cache->full_list);
        trace_inited = true;
        global_trace_cache->trace_version = 0;
    }

    return false;
}

int proxy_trace_cache_deinit()
{
    pthread_t threadid;
    trace_queue_t* trace_queue;

    mysql_mutex_lock(&global_trace_cache->trace_lock);
    if (trace_digest_inited == false)
    {
        mysql_mutex_unlock(&global_trace_cache->trace_lock);
        return false;
    }

    trace_digest_inited = false;
    mysql_mutex_unlock(&global_trace_cache->trace_lock);

    while (global_trace_cache->trace_digest_thread_stop_count < 
        global_trace_cache->trace_digest_thread_start_count)
    {
        mysql_mutex_lock(&global_trace_cache->format_thread_lock);
        mysql_cond_broadcast(&global_trace_cache->format_thread_cond);
        mysql_mutex_unlock(&global_trace_cache->format_thread_lock);
    }

    mysql_mutex_lock(&global_trace_cache->trace_lock);
    for (int i = 0; i < global_trace_cache->trace_flush_size; ++i)
    {
        my_hash_free(global_trace_cache->trace_flush[i].en_trace_hash);
        my_free(global_trace_cache->trace_flush[i].en_trace_hash);
        my_hash_free(global_trace_cache->trace_flush[i].de_trace_hash);
        my_free(global_trace_cache->trace_flush[i].de_trace_hash);

        while (global_trace_cache->trace_flush[i].running)
        {
            mysql_mutex_lock(&global_trace_cache->trace_flush[i].flush_lock);
            mysql_cond_signal(&global_trace_cache->trace_flush[i].flush_cond);
            mysql_mutex_unlock(&global_trace_cache->trace_flush[i].flush_lock);
        }

        mysql_mutex_destroy(&global_trace_cache->trace_flush[i].flush_lock);
        mysql_cond_destroy(&global_trace_cache->trace_flush[i].flush_cond);
    }
    
    mysql_mutex_destroy(&global_trace_cache->format_thread_lock);
    mysql_cond_destroy(&global_trace_cache->format_thread_cond);
    for (int i = 0; i < global_trace_cache->trace_cache_size; ++i)
    {
        trace_queue = &global_trace_cache->trace_queue[i];
        mysql_mutex_destroy(&trace_queue->trace_lock);
        for (int j=0; j < trace_queue->queue_size; ++j)
        {
            mysql_mutex_destroy(&trace_queue->trace_array[j].trace_lock);
            str_deinit(trace_queue->trace_array[j].sql_statements);
            my_free(trace_queue->trace_array[j].sql_statements);
            str_deinit(trace_queue->trace_array[j].format_sql);
            my_free(trace_queue->trace_array[j].format_sql);
            str_deinit(trace_queue->trace_array[j].errmsg);
            my_free(trace_queue->trace_array[j].errmsg);
        }

        my_free(trace_queue->trace_array);
    }
    if (global_trace_cache->trace_storage_type != PROXY_TRACE_STORAGE_MYSQL)
    {
        rd_kafka_topic_destroy((rd_kafka_topic_t*)global_trace_cache->trace_digest_rkt);
        rd_kafka_destroy((rd_kafka_t*)global_trace_cache->trace_digest_rk);
    }
    my_free(global_trace_cache->trace_queue);
    mysql_mutex_unlock(&global_trace_cache->trace_lock);
    return false;
}

int proxy_trace_digest_check_empty()
{
    trace_queue_t* trace_queue;
    mysql_mutex_lock(&global_trace_cache->trace_lock);
    for (int i = 0; i < global_trace_cache->trace_cache_size; ++i)
    {
        trace_queue = &global_trace_cache->trace_queue[i];
        if (trace_queue->dequeue_index != trace_queue->enqueue_index)
        {
            mysql_mutex_unlock(&global_trace_cache->trace_lock);
            return false;
        }
    }

    for (int i = 0; i < global_trace_cache->trace_flush_size; ++i)
    {
        if (//global_trace_cache->trace_flush[i].en_trace_hash->records > 0 ||
            global_trace_cache->trace_flush[i].de_trace_hash->records > 0)
        {
            mysql_mutex_unlock(&global_trace_cache->trace_lock);
            return false;
        }
    }

    mysql_mutex_unlock(&global_trace_cache->trace_lock);
    return true;
}

int proxy_trace_cache_init()
{
    pthread_t threadid;
    trace_queue_t* trace_queue;

    mysql_mutex_lock(&global_trace_cache->trace_lock);

    if (trace_digest_inited == true)
    {
        mysql_mutex_unlock(&global_trace_cache->trace_lock);
        return false;
    }

    trace_digest_inited = true;
    proxy_trace_cache_create();
    LIST_INIT(global_trace_cache->free_list);
    LIST_INIT(global_trace_cache->full_list);
    global_trace_cache->trace_version++;
    global_trace_cache->trace_storage_type = proxy_trace_storage;
    global_trace_cache->trace_digest_thread_start_count = 0;
    global_trace_cache->trace_digest_thread_stop_count = 0;
    global_trace_cache->local_proxy_digest_trace = proxy_digest_trace;
    global_trace_cache->trace_cache_size = proxy_trace_queue_array_length;

    /* here not to return, because proxy need the trace cache to 
     * process the rules system, and if the table and kafka can't
     * to build, then not return and continue tu build the other
     * modules, the error message will to write to error logs */
    if (global_trace_cache->trace_storage_type == PROXY_TRACE_STORAGE_MYSQL)
    {
        if (proxy_create_digest_table())
        {
            sql_print_error("proxy create trace digest table error: %s", 
                current_thd->get_stmt_da()->message());
            //mysql_mutex_unlock(&global_trace_cache->trace_lock);
            //return true;
        }
        if (proxy_create_sql_table())
        {
            sql_print_error("proxy create trace sql table error: %s", 
                current_thd->get_stmt_da()->message());
            //mysql_mutex_unlock(&global_trace_cache->trace_lock);
            //return true;
        }
    }
    else
    {
        if (proxy_trace_kafka_create("trace_digest", 
            (rd_kafka_topic_t**)&global_trace_cache->trace_digest_rkt,
            (rd_kafka_t**)&global_trace_cache->trace_digest_rk))
        {
            //mysql_mutex_unlock(&global_trace_cache->trace_lock);
            sql_print_error("proxy create trace kafka error: %s", 
                current_thd->get_stmt_da()->message());
            //return true;
        }
    }

    global_trace_cache->trace_queue = (trace_queue_t*)my_malloc(sizeof(trace_queue_t) *
        global_trace_cache->trace_cache_size, MY_ZEROFILL);
    mysql_mutex_init(NULL, &global_trace_cache->format_thread_lock, MY_MUTEX_INIT_FAST);
    mysql_cond_init(NULL, &global_trace_cache->format_thread_cond, NULL);

    for (int i = 0; i < global_trace_cache->trace_cache_size; ++i)
    {
        trace_queue = &global_trace_cache->trace_queue[i];
        trace_queue->trace_state = TRACE_STATE_EMPTY;
        trace_queue->thd_state = THD_NULL;
        mysql_mutex_init(NULL, &trace_queue->trace_lock, MY_MUTEX_INIT_FAST);
        trace_queue->queue_size = proxy_trace_client_queue_size;
        trace_queue->trace_array = (format_cache_node_t*)my_malloc(sizeof(format_cache_node_t)*
                                               trace_queue->queue_size, MY_ZEROFILL);
        for (int j=0; j < trace_queue->queue_size; ++j)
        {
            mysql_mutex_init(NULL, &trace_queue->trace_array[j].trace_lock, MY_MUTEX_INIT_FAST);
            LIST_INIT(trace_queue->trace_array[j].rt_lst);
            trace_queue->trace_array[j].state = TRACE_NODE_NULL;
            trace_queue->trace_array[j].sql_statements = 
              (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
            str_init(trace_queue->trace_array[j].sql_statements);
            trace_queue->trace_array[j].format_sql= (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
            str_init(trace_queue->trace_array[j].format_sql);
            trace_queue->trace_array[j].errmsg = (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
            str_init(trace_queue->trace_array[j].errmsg);
            trace_queue->trace_array[j].errflag = 0;
            memset(trace_queue->trace_array[j].hostname, 0, HOSTNAME_LENGTH + 1);
            memset(trace_queue->trace_array[j].dbname, 0, NAME_CHAR_LEN + 1);
            memset(trace_queue->trace_array[j].username, 0, NAME_CHAR_LEN + 1);
            memset(trace_queue->trace_array[j].client_ip, 0, 64);
            memset(trace_queue->trace_array[j].hash_key, 0, 64);
            memset(trace_queue->trace_array[j].sql_key, 0, 64);
            trace_queue->trace_array[j].count = 0;
            trace_queue->trace_array[j].last_seen = 0;
            trace_queue->trace_array[j].first_seen = 0;
            trace_queue->trace_array[j].sum_time = 0;
            trace_queue->trace_array[j].min_time = 0;
            trace_queue->trace_array[j].max_time = 0;
            trace_queue->trace_array[j].affect_rows = 0;
        }
        trace_queue->enqueue_index = trace_queue->dequeue_index = 0;
        LIST_ADD_LAST(link, global_trace_cache->free_list, trace_queue);
    }

    global_trace_cache->trace_flush_size = proxy_flush_thread_num;
    global_trace_cache->trace_flush = (trace_flush_t*)my_malloc(sizeof(trace_flush_t) *
        global_trace_cache->trace_flush_size, MY_ZEROFILL);
    for (int i = 0; i < global_trace_cache->trace_flush_size; ++i)
    {
        mysql_mutex_init(NULL, &global_trace_cache->trace_flush[i].flush_lock, MY_MUTEX_INIT_FAST);
        mysql_cond_init(NULL, &global_trace_cache->trace_flush[i].flush_cond, NULL);
        global_trace_cache->trace_flush[i].de_trace_hash = 
          (HASH*)my_malloc(sizeof(HASH), MY_ZEROFILL);
        my_hash_init(global_trace_cache->trace_flush[i].de_trace_hash, &my_charset_bin,
                  4096, 0, 0, (my_hash_get_key)proxy_trace_get_key, NULL, 0);
        global_trace_cache->trace_flush[i].en_trace_hash = 
          (HASH*)my_malloc(sizeof(HASH), MY_ZEROFILL);
        my_hash_init(global_trace_cache->trace_flush[i].en_trace_hash, &my_charset_bin,
                  4096, 0, 0, (my_hash_get_key)proxy_trace_get_key, NULL, 0);
        global_trace_cache->trace_flush[i].hash_size = proxy_trace_hash_size;
        global_trace_cache->trace_flush[i].length = proxy_trace_hash_mem_length;
        global_trace_cache->trace_flush[i].current_length = 0;
        LIST_INIT(global_trace_cache->trace_flush[i].en_free_list);
        LIST_INIT(global_trace_cache->trace_flush[i].de_free_list);
        global_trace_cache->trace_flush[i].start_time = my_hrtime().val;
        mysql_thread_create(0, &threadid, &connection_attrib,
            proxy_trace_flush_thread, (void*)&global_trace_cache->trace_flush[i]);
    }
    
    for (ulong i = 0; i < proxy_format_thread_num; ++i)
        mysql_thread_create(0, &threadid, &connection_attrib,
            proxy_trace_format_thread, NULL);

    /*wait to create complete */
    while (global_trace_cache->trace_digest_thread_start_count != (int)proxy_format_thread_num)
        sleep(0);

    mysql_mutex_unlock(&global_trace_cache->trace_lock);
    return false;
}

int proxy_assign_thd_trace(THD* thd)
{
    if (thd->connection_type != PROXY_CONNECT_PROXY)
        return false;

    trace_queue_t* trace_queue = NULL;
    mysql_mutex_lock(&global_trace_cache->trace_lock);
    /* in the mutex, if the digest is closed, then not to record for this thd */
    if (!proxy_digest_cache_on())
    {
        mysql_mutex_unlock(&global_trace_cache->trace_lock);
        return true;
    }

    trace_queue = LIST_GET_FIRST(global_trace_cache->free_list);
    if (trace_queue == NULL)
    {
        mysql_mutex_unlock(&global_trace_cache->trace_lock);
        return true;
    }
    thd->trace_version = proxy_trace_version();
    thd->trace_queue = trace_queue;
    trace_queue->trace_state = TRACE_STATE_BUSY;
    trace_queue->thd_state = THD_RUNNING;
    trace_queue->thd = thd;
    trace_queue->thread_id = thd->thread_id;
    LIST_REMOVE(link, global_trace_cache->free_list, trace_queue);
    LIST_ADD_FIRST(link, global_trace_cache->full_list, trace_queue);
    mysql_mutex_unlock(&global_trace_cache->trace_lock);
    return false;
}

format_cache_node_t*
format_cache_get_node(THD* thd)
{
    int dequeue_index;
    int enqueue_index;
    format_cache_node_t* node = NULL;

retry:
    dequeue_index = thd->trace_queue->dequeue_index;
    enqueue_index = thd->trace_queue->enqueue_index;
    if ((enqueue_index+1) % thd->trace_queue->queue_size != dequeue_index)
    {
        node = &thd->trace_queue->trace_array[enqueue_index];
        node->count = 0;
        node->last_seen = 0;
        node->first_seen = 0;
        node->sum_time = 0;
        node->min_time = 0;
        node->max_time = 0;
        node->errflag = 0;
        node->rule_logged = false;
        node->affect_rows = 0;
        memset(node->hash_key, 0, 64);
        memset(node->sql_key, 0, 64);
        memset(node->client_ip, 0, 64);
        memset(node->hostname, 0, HOSTNAME_LENGTH + 1);
        memset(node->dbname, 0, NAME_CHAR_LEN + 1);
        memset(node->username, 0, NAME_CHAR_LEN + 1);
        str_truncate_0(node->sql_statements);
        str_truncate_0(node->format_sql);
        str_truncate_0(node->errmsg);
        thd->trace_queue->count++;
        deinit_format_cache_node(node, false);

        return node;
    }
    else
    {
        if (proxy_busy_wait())
            goto retry;
    }
    return NULL;
}

int proxy_format_to_queue_before(THD* thd)
{
    format_cache_node_t* node;

    if (thd->trace_queue == NULL)
        return true;

    node = format_cache_get_node(thd);
    if (node == NULL)
        return true;

    mysql_format_command(thd, node);
    node->digest = proxy_get_digest(node);
    if (thd->query_rule)
    {
        thd->query_rule->digest = node->digest;
        thd->query_rule->digest_text = (char*)my_malloc(str_get_len(node->format_sql) + 10, MYF(0));
        memcpy(thd->query_rule->digest_text, str_get(node->format_sql), 
            str_get_len(node->format_sql));
        thd->query_rule->digest_text[str_get_len(node->format_sql)] = '\0';
    }
    return false;
}

int proxy_format_to_queue_after(THD* thd, ulonglong exe_time, ulonglong affect_rows, const char* server_name)
{
    if (thd->trace_queue == NULL)
        return true;

    if (proxy_digest_on(thd))
    {
        format_cache_node_t* node = &thd->trace_queue->trace_array[thd->trace_queue->enqueue_index];
        if (str_get_len(node->format_sql) == 0)
            return false;

        node->sum_time = exe_time;
        node->min_time = exe_time;
        node->max_time = exe_time;
        node->affect_rows = affect_rows;
        node->state = TRACE_NODE_FULL;
        thd->trace_queue->enqueue_index = (thd->trace_queue->enqueue_index + 1) % 
          thd->trace_queue->queue_size;
        strcpy(node->route_server_name, server_name);
    }

    return false;
}

int proxy_trace_hash_wait_or_switch(
    trace_flush_t* trace_flush, 
    int* switch_flag
)
{
    HASH* hash;
    LIST_BASE_NODE_T(format_cache_node_t) tmp_list;
    int waitandretry = false;

    /* 如果到时间了，要切换缓存 
     * 如果个数超过设定了，就切换缓存
     * 如果占用内存数量超过设定了，就切换缓存 
     * */
retry:
    if ((my_hrtime().val - trace_flush->start_time > proxy_trace_max_delay_time * 1000 &&
        trace_flush->en_trace_hash->records > 0) ||
        trace_flush->current_length > trace_flush->length ||
        trace_flush->en_trace_hash->records > trace_flush->hash_size)
    {
        /* 如果flush操作正在做，则处理方法有两种：等待，或者忽略
         * 如果不在使用，则可以切换 */
        if (trace_flush->detrace == true)
        {
            mysql_mutex_unlock(&trace_flush->flush_lock);
            if (proxy_busy_wait() && !waitandretry)
            {
                waitandretry = true;
                mysql_mutex_lock(&trace_flush->flush_lock);
                goto retry;
            }
            else
            {
                mysql_mutex_lock(&trace_flush->flush_lock);
                return true;
            }
        }
        else
        {
            /* 交换信息 */
            hash = trace_flush->en_trace_hash;
            trace_flush->en_trace_hash = trace_flush->de_trace_hash;
            trace_flush->de_trace_hash = hash;
            /* 这样交换行不行？*/
            tmp_list.count = trace_flush->en_free_list.count;
            trace_flush->en_free_list.count = trace_flush->de_free_list.count;
            trace_flush->de_free_list.count = tmp_list.count;
            tmp_list.start = trace_flush->en_free_list.start;
            trace_flush->en_free_list.start= trace_flush->de_free_list.start;
            trace_flush->de_free_list.start= tmp_list.start;
            tmp_list.end = trace_flush->en_free_list.end;
            trace_flush->en_free_list.end= trace_flush->de_free_list.end;
            trace_flush->de_free_list.end= tmp_list.end;

            trace_flush->start_time = my_hrtime().val;
        }
    }
    return false;
}

int proxy_flush_to_bucklet(trace_sql_bucket_t* trace_sql_bucket, format_cache_node_t* trace_node)
{
    size_t length;
    format_cache_node_t* trace_node_local;
    int flush_index = my_hrtime().val % global_trace_cache->trace_sql_bucket_size;
    int dequeue_index;
    int enqueue_index;
    
retry:
    mysql_mutex_lock(&trace_sql_bucket[flush_index].bucket_lock);
    dequeue_index = trace_sql_bucket[flush_index].bucket_dequeue_index;
    enqueue_index = trace_sql_bucket[flush_index].bucket_enqueue_index;
    if ((enqueue_index+1) % trace_sql_bucket[flush_index].bucket_size != dequeue_index)
    {
        trace_node_local = &trace_sql_bucket[flush_index].sql_bucket[enqueue_index];
        trace_sql_bucket[flush_index].bucket_enqueue_index =
                                (enqueue_index + 1) % trace_sql_bucket[flush_index].bucket_size;
        mysql_mutex_unlock(&trace_sql_bucket[flush_index].bucket_lock);
        str_truncate_0(trace_node_local->sql_statements);
        str_truncate_0(trace_node_local->format_sql);
        str_append_with_length(trace_node_local->format_sql,
                               str_get(trace_node->format_sql),
                               str_get_len(trace_node->format_sql));
        str_append_with_length(trace_node_local->sql_statements,
                               str_get(trace_node->sql_statements),
                               str_get_len(trace_node->sql_statements));
        trace_sql_bucket->current_length += str_get_alloc_len(trace_node_local->sql_statements)
        - str_get_last_len(trace_node_local->sql_statements)
        + str_get_alloc_len(trace_node_local->sql_statements)
        - str_get_last_len(trace_node_local->sql_statements);
        str_set_last_len(trace_node_local->sql_statements);
        str_set_last_len(trace_node_local->format_sql);
        strcpy(trace_node_local->hostname, trace_node->hostname);
        strcpy(trace_node_local->client_ip, trace_node->client_ip);
        strcpy(trace_node_local->username, trace_node->username);
        strcpy(trace_node_local->dbname, trace_node->dbname);
        strcpy(trace_node_local->hash_key, trace_node->hash_key);
        strcpy(trace_node_local->route_server_name, trace_node->route_server_name);
        trace_node_local->first_seen = my_hrtime().val;
        trace_node_local->last_seen = trace_node_local->first_seen;
        trace_node_local->count = 1;
        trace_node_local->sum_time = trace_node->sum_time;
        trace_node_local->min_time = trace_node->min_time;
        trace_node_local->max_time = trace_node->max_time;
        trace_node_local->affect_rows = trace_node->affect_rows;
        trace_node_local->state = TRACE_NODE_FULL;
        proxy_sql_get_key(trace_node_local->sql_key,str_get(trace_node->sql_statements),
                          str_get_len(trace_node->sql_statements), &length, 0);
    }
    else
    {
        mysql_mutex_unlock(&trace_sql_bucket[flush_index].bucket_lock);
        if (proxy_busy_wait())
            goto retry;
    }

    return false;
}

int proxy_format_and_flush_to(THD* thd, format_cache_node_t* trace_node)
{
    int idx, ret;
    size_t length;
    format_cache_node_t* trace_node_local;
    my_hash_value_type hash_value;
    trace_flush_t* trace_flush;
    int switch_flag = false;

    /* 这里先做简单处理，为了实现二级HASH，先使用global_trace_cache->trace_flush[0]
     * 计算一个hash_value出来，然后再用这个值模trace_flush_size，然后再找到具体的HASH */
    /* 因为下面有可能已经做了switch了，所以HASH表的映射关系，需要重新计算 */
    proxy_trace_get_key(trace_node, &length, 0);
    trace_flush = &global_trace_cache->trace_flush[0];
    hash_value= my_calc_hash(trace_flush->en_trace_hash,
        (uchar*) trace_node->hash_key, length);
    idx = hash_value % global_trace_cache->trace_flush_size;
    trace_flush = &global_trace_cache->trace_flush[idx];

retry:
    hash_value= my_calc_hash(trace_flush->en_trace_hash,
        (uchar*) trace_node->hash_key, length);
    mysql_mutex_lock(&trace_flush->flush_lock);
    trace_node_local = (format_cache_node_t*)my_hash_search_using_hash_value(
        trace_flush->en_trace_hash, hash_value, (uchar*) trace_node->hash_key, length);
    /* 结果有：等待成功（0）、忽略+等待超时（1）、切换成功（0） */
    if ((ret = proxy_trace_hash_wait_or_switch(trace_flush, &switch_flag)) == true)
    {
        mysql_mutex_unlock(&trace_flush->flush_lock);
        return false;
    }
    if (!trace_node_local)
    {
        if (switch_flag)
        {
            mysql_mutex_unlock(&trace_flush->flush_lock);
            goto retry;
        }

        if (LIST_GET_LEN(trace_flush->en_free_list) == 0)
        {
            trace_node_local = (format_cache_node_t*)my_malloc(sizeof(format_cache_node_t),
                MY_ZEROFILL);
            trace_node_local->sql_statements = (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
            str_init(trace_node_local->sql_statements);
            trace_node_local->format_sql= (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
            str_init(trace_node_local->format_sql);
            mysql_mutex_init(NULL, &trace_node_local->trace_lock, MY_MUTEX_INIT_FAST);
            str_append_with_length(trace_node_local->format_sql,
                                   str_get(trace_node->format_sql), 
                                   str_get_len(trace_node->format_sql));
            str_append_with_length(trace_node_local->sql_statements,
                                   str_get(trace_node->sql_statements), 
                                   str_get_len(trace_node->sql_statements));
            trace_flush->current_length += sizeof(*trace_node_local)
                                   + str_get_alloc_len(trace_node_local->sql_statements)
                                   + str_get_alloc_len(trace_node_local->format_sql);
        }
        else
        {
            trace_node_local = LIST_GET_FIRST(trace_flush->en_free_list);
            LIST_REMOVE(link, trace_flush->en_free_list, trace_node_local);
            str_truncate_0(trace_node_local->sql_statements);
            str_truncate_0(trace_node_local->format_sql);
            str_append_with_length(trace_node_local->format_sql,
                                   str_get(trace_node->format_sql), 
                                   str_get_len(trace_node->format_sql));
            str_append_with_length(trace_node_local->sql_statements,
                                   str_get(trace_node->sql_statements), 
                                   str_get_len(trace_node->sql_statements));
            trace_flush->current_length += str_get_alloc_len(trace_node->sql_statements)
                                   - str_get_last_len(trace_node->sql_statements)
                                   + str_get_alloc_len(trace_node->sql_statements)
                                   - str_get_last_len(trace_node->sql_statements);
            str_set_last_len(trace_node->sql_statements);
            str_set_last_len(trace_node->format_sql);
        }

        strcpy(trace_node_local->hostname, trace_node->hostname);
        strcpy(trace_node_local->client_ip, trace_node->client_ip);
        strcpy(trace_node_local->username, trace_node->username);
        strcpy(trace_node_local->dbname, trace_node->dbname);
        strcpy(trace_node_local->hash_key, trace_node->hash_key);
        strcpy(trace_node_local->route_server_name, trace_node->route_server_name);
        trace_node_local->first_seen = my_hrtime().val;
        trace_node_local->last_seen = trace_node_local->first_seen;
        trace_node_local->count = 1;
        trace_node_local->sum_time = trace_node->sum_time;
        trace_node_local->min_time = trace_node->min_time;
        trace_node_local->max_time = trace_node->max_time;
        trace_node_local->digest = trace_node->digest;
        trace_node_local->affect_rows = trace_node->affect_rows;
        trace_node_local->state = trace_node->state = TRACE_NODE_FULL;
        my_hash_insert(trace_flush->en_trace_hash, (uchar*)trace_node_local);
    }
    else
    {
        trace_node_local->last_seen = my_hrtime().val;
        trace_node_local->count += 1;
        trace_node_local->sum_time += trace_node->sum_time;
        if (trace_node->min_time < trace_node_local->min_time)
            trace_node_local->min_time = trace_node->min_time;
        if (trace_node->max_time > trace_node_local->max_time)
            trace_node_local->max_time = trace_node->max_time;
        trace_node_local->affect_rows = trace_node->affect_rows;
        trace_node_local->state = TRACE_NODE_FULL;
    }
    mysql_mutex_unlock(&trace_flush->flush_lock);

    if (proxy_trace_sql_on(trace_node->rule_logged))
        proxy_flush_to_bucklet(global_trace_cache->trace_sql_bucket, trace_node);

    return false;
}

pthread_handler_t proxy_trace_format_thread(void* arg)
{
    THD *exe_thd= NULL;
    format_cache_node_t* trace_node;
    struct timespec abstime;
    my_thread_init();
    exe_thd = new THD(0);
    exe_thd->thread_stack= (char*) &exe_thd;
    setup_connection_thread_globals(exe_thd);
    trace_queue_t* trace_queue = NULL;

    mysql_mutex_lock(&LOCK_thread_count);
    global_trace_cache->trace_digest_thread_start_count++;
    mysql_mutex_unlock(&LOCK_thread_count);

    int i = 0;
    while (!abort_loop && trace_digest_inited)
    {
        mysql_mutex_lock(&global_trace_cache->format_thread_lock);
        set_timespec_nsec(abstime, proxy_trace_format_thread_sleep_msec * 1000000ULL);
        mysql_cond_timedwait(&global_trace_cache->format_thread_cond, 
            &global_trace_cache->format_thread_lock, &abstime);
        mysql_mutex_unlock(&global_trace_cache->format_thread_lock);

        if (i == global_trace_cache->trace_cache_size)
            i = 0;

        trace_queue = &global_trace_cache->trace_queue[i++];
        mysql_mutex_lock(&trace_queue->trace_lock);
        
        if (trace_queue->trace_state != TRACE_STATE_BUSY)
        {
            if (trace_queue->trace_state == TRACE_STATE_EMPTY)
                i = 0;
            mysql_mutex_unlock(&trace_queue->trace_lock);
            continue;
        }
        
        trace_queue->trace_state = TRACE_STATE_FORMAT;
        mysql_mutex_unlock(&trace_queue->trace_lock);

        for (ulong j = 0; j < proxy_format_num_per_time; j++)
        {
            /* if current queue is not ready, goto next queue */
            if (trace_queue->dequeue_index != trace_queue->enqueue_index)
            {
                trace_node = &trace_queue->trace_array[trace_queue->dequeue_index];
            }
            else
            {
                if (trace_queue->thd_state == THD_END)
                {
                    trace_queue->trace_state = TRACE_STATE_RESET;
                    trace_queue->thd_state = THD_NULL;
                    mysql_mutex_lock(&global_trace_cache->trace_lock);
                    LIST_REMOVE(link, global_trace_cache->full_list, trace_queue);
                    LIST_ADD_FIRST(link, global_trace_cache->free_list, trace_queue);
                    mysql_mutex_unlock(&global_trace_cache->trace_lock);
                }
                break;
            }

            /* flush to hash */
            proxy_format_and_flush_to(exe_thd, trace_node);
            trace_node->state = TRACE_NODE_NULL;
            trace_queue->dequeue_index = (trace_queue->dequeue_index+1) % trace_queue->queue_size;
        }

        if (trace_queue->thd_state != THD_NULL)
            trace_queue->trace_state = TRACE_STATE_BUSY;
    }


    mysql_mutex_lock(&LOCK_thread_count);
    global_trace_cache->trace_digest_thread_stop_count++;
    exe_thd->unlink();
    mysql_mutex_unlock(&LOCK_thread_count);
    delete exe_thd;
    my_thread_end();
    pthread_exit(0);
    return NULL;
}

int
proxy_create_digest_table()
{
    backend_conn_t* conn;
    conn = (backend_conn_t*)my_malloc(sizeof(backend_conn_t), MY_ZEROFILL);
    conn->set_mysql(proxy_get_trace_server_connection_with_con(conn));
    if (conn->get_mysql() == NULL)
    {
        my_free(conn);
        return true;
    }

    str_t str_sql;
    str_t str_createdb;
    str_init(&str_sql);
    str_init(&str_createdb);
    char dbname[NAME_LEN + 1];
    int had_config = proxy_get_config_database(dbname);

    str_append(&str_sql, "CREATE TABLE ");
    str_append(&str_sql, dbname);
    str_append(&str_sql, ".query_digest");
    if (!had_config)
    {
        str_append(&str_createdb, "CREATE DATABASE ");
        str_append(&str_createdb, proxy_namespace);
        
        if (mysql_real_query((MYSQL*)conn->get_mysql(), str_get(&str_createdb), 
              str_get_len(&str_createdb)) &&
            mysql_errno((MYSQL*)conn->get_mysql()) != 1007/*ER_DB_CREATE_EXISTS*/)
        {
            my_error(ER_NO_BACKEND_FOUND, MYF(0), mysql_error((MYSQL*)conn->get_mysql()));
            str_deinit(&str_sql);
            str_deinit(&str_createdb);
            close_backend_conn(conn);
            my_free(conn);
            return true;
        }
    }

    str_append(&str_sql, "(id bigint unsigned NOT NULL AUTO_INCREMENT comment 'auto increment', ");
    str_append(&str_sql, "schemaname varchar(65) not null comment 'db name', ");
    str_append(&str_sql, "username varchar(65) not null comment 'user name', ");
    str_append(&str_sql, "digest varchar(65) not null comment 'hash key', ");
    str_append(&str_sql, "formatsql text comment 'format sql', ");
    str_append(&str_sql, "count bigint not null comment 'sum of sql', ");
    str_append(&str_sql, "first_seen bigint not null comment 'first_seen', ");
    str_append(&str_sql, "last_seen bigint not null comment 'last_seen', ");
    str_append(&str_sql, "sum_time bigint not null comment 'sum of sql time', ");
    str_append(&str_sql, "min_time bigint not null comment 'min time per sql', ");
    str_append(&str_sql, "max_time bigint not null comment 'max time per sql', ");
    str_append(&str_sql, "primary key (id),");
    str_append(&str_sql, "unique key uniq_digest (digest))");
    str_append(&str_sql, "engine innodb comment 'digest table'");
    
    if (mysql_real_query((MYSQL*)conn->get_mysql(), str_get(&str_sql), str_get_len(&str_sql)) &&
        mysql_errno((MYSQL*)conn->get_mysql()) != 1050/*ER_TABLE_EXISTS_ERROR*/)
    {
        my_error(ER_NO_BACKEND_FOUND, MYF(0), mysql_error((MYSQL*)conn->get_mysql()));
        str_deinit(&str_sql);
        str_deinit(&str_createdb);
        close_backend_conn(conn);
        my_free(conn);
        return true;
    }
    
    str_deinit(&str_sql);
    str_deinit(&str_createdb);
    close_backend_conn(conn);
    my_free(conn);
    return false;
}

int proxy_trace_flush_kafka(format_cache_node_t* trace_node, str_t* digest)
{
    char dbname[NAME_LEN + 1];
    rd_kafka_topic_t* rkt;
    rd_kafka_t* rk;

    str_truncate_0(digest);
    str_append(digest, "{");
    str_append(digest, "\"dbname\":");
    str_append(digest, "\"");
    str_append(digest, trace_node->dbname);
    str_append(digest, "\",");
    str_append(digest, "\"username\":");
    str_append(digest, "\"");
    str_append(digest, trace_node->username);
    str_append(digest, "\",");
    str_append(digest, "\"digest\":");
    str_append(digest, "\"");
    sprintf(dbname,"0x%016llX", (long long unsigned int)trace_node->digest);
    str_append(digest, dbname);
    str_append(digest, "\",");
    str_append(digest, "\"formatsql\":");
    str_append(digest, "\"");
    str_append_with_length(digest, str_get(trace_node->format_sql),
                           str_get_len(trace_node->format_sql));
    str_append(digest, "\",");

    str_append(digest, "\"count\":");
    sprintf(dbname, "%lu,", trace_node->count);
    str_append(digest, dbname);

    str_append(digest, "\"first_seen\":");
    sprintf(dbname, "%llu,", trace_node->first_seen);
    str_append(digest, dbname);

    str_append(digest, "\"last_seen\":");
    sprintf(dbname, "%llu,", trace_node->last_seen);
    str_append(digest, dbname);

    str_append(digest, "\"sum_time\":");
    sprintf(dbname, "%llu,", trace_node->sum_time);
    str_append(digest, dbname);

    str_append(digest, "\"min_time\":");
    sprintf(dbname, "%llu,", trace_node->min_time);
    str_append(digest, dbname);

    str_append(digest, "\"max_time\":");
    sprintf(dbname, "%llu", trace_node->max_time);
    str_append(digest, dbname);

    str_append(digest, "}");

    rkt = (rd_kafka_topic_t*)global_trace_cache->trace_digest_rkt;
    rk = (rd_kafka_t*)global_trace_cache->trace_digest_rk;
    if (rkt && rk)
    {
        proxy_kafka_produce(rkt, rk, digest);
    }

    return false;
}

int proxy_trace_flush(format_cache_node_t* trace_node, str_t* digest, MYSQL* conn)
{
    char dbname[NAME_LEN + 1];
    proxy_get_config_database(dbname);
    str_truncate_0(digest);
    str_append(digest, "INSERT INTO ");
    str_append(digest, dbname);
    str_append(digest, ".query_digest (schemaname, username, digest, formatsql,"
               "count, first_seen, last_seen, sum_time, min_time, max_time) VALUES(");
    str_append(digest, "\'");
    str_append(digest, trace_node->dbname);
    str_append(digest, "\',");
    str_append(digest, "\'");
    str_append(digest, trace_node->username);
    str_append(digest, "\',");
    str_append(digest, "\'");
    sprintf(dbname,"0x%016llX", (long long unsigned int)trace_node->digest);
    str_append(digest, dbname);
    str_append(digest, "\',");
    str_append(digest, "\'");
    str_append_with_length(digest, str_get(trace_node->format_sql),
                           str_get_len(trace_node->format_sql));
    str_append(digest, "\',");
    sprintf(dbname, "%lu,", trace_node->count);
    str_append(digest, dbname);
    sprintf(dbname, "%llu,", trace_node->first_seen);
    str_append(digest, dbname);
    sprintf(dbname, "%llu,", trace_node->last_seen);
    str_append(digest, dbname);
    sprintf(dbname, "%llu,", trace_node->sum_time);
    str_append(digest, dbname);
    sprintf(dbname, "%llu,", trace_node->min_time);
    str_append(digest, dbname);
    sprintf(dbname, "%llu)", trace_node->max_time);
    str_append(digest, dbname);
    str_append(digest, " ON DUPLICATE KEY UPDATE count = count + ");
    sprintf(dbname, "%lu", trace_node->count);
    trace_node->count = 0;
    str_append(digest, dbname);
    str_append(digest, " , last_seen = ");
    sprintf(dbname, "%llu", trace_node->last_seen);
    str_append(digest, dbname);
    str_append(digest, " , sum_time = sum_time + ");
    sprintf(dbname, "%llu", trace_node->sum_time);
    str_append(digest, dbname);
    str_append(digest, " , min_time = (CASE WHEN ");
    sprintf(dbname, "%llu", trace_node->min_time);
    str_append(digest, dbname);
    str_append(digest, " < min_time then ");
    str_append(digest, dbname);
    str_append(digest, " else min_time END)");
    str_append(digest, " , max_time = (CASE WHEN ");
    sprintf(dbname, "%llu", trace_node->max_time);
    str_append(digest, dbname);
    str_append(digest, " > max_time then ");
    str_append(digest, dbname);
    str_append(digest, " else max_time END)");
    
    if (mysql_real_query(conn, str_get(digest), str_get_len(digest)))
    {
        sql_print_error("Trace log write error. %s",mysql_error(conn));
    }
    return false;
}

int
proxy_create_sql_table()
{
    backend_conn_t* conn;
    conn = (backend_conn_t*)my_malloc(sizeof(backend_conn_t), MY_ZEROFILL);
    conn->set_mysql(proxy_get_trace_server_connection_with_con(conn));
    if (conn->get_mysql() == NULL)
    {
        my_free(conn);
        return true;
    }
    str_t str_sql;
    str_t str_createdb;
    str_init(&str_sql);
    str_init(&str_createdb);
    char dbname[NAME_LEN + 1];
    int had_config = proxy_get_config_database(dbname);
    
    str_append(&str_sql, "CREATE TABLE ");
    str_append(&str_sql, dbname);
    str_append(&str_sql, ".query_sql");
    if (!had_config)
    {
        str_append(&str_createdb, "CREATE DATABASE ");
        str_append(&str_createdb, proxy_namespace);
        
        if (mysql_real_query((MYSQL*)conn->get_mysql(), str_get(&str_createdb),
                             str_get_len(&str_createdb)) &&
            mysql_errno((MYSQL*)conn->get_mysql()) != 1007/*ER_DB_CREATE_EXISTS*/)
        {
            my_error(ER_NO_BACKEND_FOUND, MYF(0), mysql_error((MYSQL*)conn->get_mysql()));
            str_deinit(&str_sql);
            str_deinit(&str_createdb);
            close_backend_conn(conn);
            my_free(conn);
            return true;
        }
    }
    
    str_append(&str_sql, "(id bigint unsigned NOT NULL AUTO_INCREMENT comment 'auto increment', ");
    str_append(&str_sql, "schemaname varchar(65) not null comment 'db name', ");
    str_append(&str_sql, "client_ip varchar(65) not null comment 'client ip', ");
    str_append(&str_sql, "username varchar(65) not null comment 'user name', ");
    str_append(&str_sql, "sql_key varchar(65) not null comment 'hash key', ");
    str_append(&str_sql, "digest varchar(65) not null comment 'digest hash key', ");
    str_append(&str_sql, "origin_sql text comment 'origin sql', ");
    str_append(&str_sql, "affect_rows bigint not null comment 'affect rows', ");
    str_append(&str_sql, "appear_ts bigint not null comment 'appear timestamp', ");
    str_append(&str_sql, "exe_time bigint not null comment 'sum of sql time', ");
    str_append(&str_sql, "route_server_name varchar(65) not null comment 'route server name', ");
    str_append(&str_sql, "primary key (id))");
    str_append(&str_sql, "engine innodb comment 'origin sql table'");
    
    if (mysql_real_query((MYSQL*)conn->get_mysql(), str_get(&str_sql), str_get_len(&str_sql)) &&
        mysql_errno((MYSQL*)conn->get_mysql()) != 1050/*ER_TABLE_EXISTS_ERROR*/)
    {
        my_error(ER_NO_BACKEND_FOUND, MYF(0), mysql_error((MYSQL*)conn->get_mysql()));
        str_deinit(&str_sql);
        str_deinit(&str_createdb);
        close_backend_conn(conn);
        my_free(conn);
        return true;
    }
    
    str_deinit(&str_sql);
    str_deinit(&str_createdb);
    close_backend_conn(conn);
    my_free(conn);
    return false;
}

int proxy_trace_sql_flush_kafka(format_cache_node_t* trace_node, str_t* digest)
{
    char* dupcharsql_1;
    char* dupcharsql_2;
    char dbname[NAME_LEN + 1];
    rd_kafka_topic_t* rkt;
    rd_kafka_t* rk;

    str_truncate_0(digest);
    str_append(digest, "{");
    str_append(digest, "\"schemaname\":");
    str_append(digest, "\"");
    str_append(digest, trace_node->dbname);
    str_append(digest, "\",");
    str_append(digest, "\"client_ip\":");
    str_append(digest, "\"");
    str_append(digest, trace_node->client_ip);
    str_append(digest, "\",");
    str_append(digest, "\"username\":");
    str_append(digest, "\"");
    str_append(digest, trace_node->username);
    str_append(digest, "\",");
    str_append(digest, "\"sql_key\":");
    str_append(digest, "\"");
    str_append(digest, trace_node->sql_key);
    str_append(digest, "\",");
    str_append(digest, "\"digest_key\":");
    str_append(digest, "\"");
    str_append(digest, trace_node->hash_key);
    str_append(digest, "\",");
    
    dupcharsql_1 = (char*)my_malloc(str_get_len(trace_node->sql_statements) * 2 + 1, MYF(0));
    memset(dupcharsql_1, 0, str_get_len(trace_node->sql_statements) * 2 + 1);
    mysql_dup_char_with_len(str_get(trace_node->sql_statements), dupcharsql_1, '"',
                            str_get_len(trace_node->sql_statements));
    str_truncate_0(trace_node->sql_statements);
    str_append_with_length(trace_node->sql_statements, dupcharsql_1, strlen(dupcharsql_1));
    my_free(dupcharsql_1);

    dupcharsql_2 = (char*)my_malloc(str_get_len(trace_node->sql_statements) * 2 + 1, MYF(0));
    memset(dupcharsql_2, 0, str_get_len(trace_node->sql_statements) * 2 + 1);
    mysql_dup_char_with_len(str_get(trace_node->sql_statements), dupcharsql_2, '\\',
                            str_get_len(trace_node->sql_statements));
    str_truncate_0(trace_node->sql_statements);
    str_append_with_length(trace_node->sql_statements, dupcharsql_2, strlen(dupcharsql_2));
    my_free(dupcharsql_2);
    
    str_append(digest, "\"origin_sql\":");
    str_append(digest, "\"");
    str_append_with_length(digest, str_get(trace_node->sql_statements),
                           str_get_len(trace_node->sql_statements));
    str_append(digest, "\",");

    str_append(digest, "\"affect_rows\":");
    sprintf(dbname, "%llu,", trace_node->affect_rows);
    str_append(digest, dbname);

    str_append(digest, "\"appear_ts\":");
    sprintf(dbname, "%llu,", trace_node->first_seen);
    str_append(digest, dbname);

    str_append(digest, "\"exe_time\":");
    sprintf(dbname, "%llu", trace_node->max_time);
    str_append(digest, dbname);
    str_append(digest, "}");

    rkt = (rd_kafka_topic_t*)global_trace_cache->trace_sql_rkt;
    rk = (rd_kafka_t*)global_trace_cache->trace_sql_rk;
    if (rkt && rk)
    {
        proxy_kafka_produce(rkt, rk, digest);
    }

    return false;
}

int proxy_trace_sql_flush(format_cache_node_t* trace_node, str_t* digest, MYSQL* conn)
{
    char* dupcharsql_1;
    char* dupcharsql_2;
    char dbname[NAME_LEN + 1];
    proxy_get_config_database(dbname);
    str_truncate_0(digest);
    str_append(digest, "INSERT INTO ");
    str_append(digest, dbname);
    str_append(digest, ".query_sql (schemaname, client_ip, username, "
        "sql_key, digest, origin_sql,"
        "affect_rows, appear_ts, exe_time, route_server_name) VALUES(");
    str_append(digest, "\'");
    str_append(digest, trace_node->dbname);
    str_append(digest, "\',");
    str_append(digest, "\'");
    str_append(digest, trace_node->client_ip);
    str_append(digest, "\',");
    str_append(digest, "\'");
    str_append(digest, trace_node->username);
    str_append(digest, "\',");
    str_append(digest, "\'");
    str_append(digest, trace_node->sql_key);
    str_append(digest, "\',");
    str_append(digest, "\'");
    str_append(digest, trace_node->hash_key);
    str_append(digest, "\',");
    str_append(digest, "\'");

    dupcharsql_1 = (char*)my_malloc(str_get_len(trace_node->sql_statements) * 2 + 1, MYF(0));
    memset(dupcharsql_1, 0, str_get_len(trace_node->sql_statements) * 2 + 1);
    mysql_dup_char_with_len(str_get(trace_node->sql_statements), dupcharsql_1, '\'',
                            str_get_len(trace_node->sql_statements));
    str_truncate_0(trace_node->sql_statements);
    str_append_with_length(trace_node->sql_statements, dupcharsql_1, strlen(dupcharsql_1));
    my_free(dupcharsql_1);

    dupcharsql_2 = (char*)my_malloc(str_get_len(trace_node->sql_statements) * 2 + 1, MYF(0));
    memset(dupcharsql_2, 0, str_get_len(trace_node->sql_statements) * 2 + 1);
    mysql_dup_char_with_len(str_get(trace_node->sql_statements), dupcharsql_2, '\\',
                            str_get_len(trace_node->sql_statements));
    str_truncate_0(trace_node->sql_statements);
    str_append_with_length(trace_node->sql_statements, dupcharsql_2, strlen(dupcharsql_2));
    my_free(dupcharsql_2);

    str_append_with_length(digest, str_get(trace_node->sql_statements),
                           str_get_len(trace_node->sql_statements));
    str_append(digest, "\',");
    sprintf(dbname, "%llu,", trace_node->affect_rows);
    str_append(digest, dbname);
    sprintf(dbname, "%llu,", trace_node->first_seen);
    str_append(digest, dbname);
    sprintf(dbname, "%llu,", trace_node->max_time);
    str_append(digest, dbname);
    str_append(digest, "\'");
    str_append(digest, trace_node->route_server_name);
    str_append(digest, "\')");

    if (mysql_real_query(conn, str_get(digest), str_get_len(digest)))
    {
        sql_print_error("Trace general log write error. %s",mysql_error(conn));
    }
    return false;
}

pthread_handler_t proxy_trace_flush_thread(void* arg)
{
    THD *exe_thd= NULL;
    trace_flush_t* flush_node;
    format_cache_node_t* trace_node;
    str_t digest;
    struct timespec abstime;
    str_init(&digest);
    backend_conn_t* conn;
    conn = (backend_conn_t*)my_malloc(sizeof(backend_conn_t), MY_ZEROFILL);
    my_thread_init();
    exe_thd = new THD(0);
    exe_thd->thread_stack= (char*) &exe_thd;

    flush_node = (trace_flush_t*)arg;
    setup_connection_thread_globals(exe_thd);
    flush_node->running = true;

    while (!abort_loop && trace_digest_inited)
    {
        mysql_mutex_lock(&flush_node->flush_lock);        
        if (flush_node->de_trace_hash->records > 0)
            flush_node->detrace = true;
        else
            flush_node->detrace = false;
        mysql_mutex_unlock(&flush_node->flush_lock);

        /* 如果当前没有要去刷盘的，就等等 */
        if (flush_node->detrace == false)
        {
            mysql_mutex_lock(&flush_node->flush_lock);
            set_timespec_nsec(abstime, proxy_trace_flush_thread_sleep_msec * 1000000ULL);
            mysql_cond_timedwait(&flush_node->flush_cond, &flush_node->flush_lock, &abstime);
            mysql_mutex_unlock(&flush_node->flush_lock);
            continue;
        }

        for (uint i=0; i < flush_node->de_trace_hash->records; i++)
        {
            trace_node = (format_cache_node_t*)my_hash_element(flush_node->de_trace_hash, i);
            /* 防止timeout，再拿一次 , 如果连接还是没有拿到，如何处理？*/
            if (global_trace_cache->trace_storage_type == PROXY_TRACE_STORAGE_MYSQL)
            {
                conn->set_mysql(proxy_get_trace_server_connection_with_con(conn));
                if (conn->get_mysql() == NULL)
                {
                    mysql_mutex_lock(&flush_node->flush_lock);
                    set_timespec_nsec(abstime, proxy_trace_flush_thread_sleep_msec * 1000000ULL);
                    mysql_cond_timedwait(&flush_node->flush_cond, &flush_node->flush_lock, &abstime);
                    mysql_mutex_unlock(&flush_node->flush_lock);
                    if (trace_digest_inited)
                        break;
                    continue;
                }
                proxy_trace_flush(trace_node, &digest, (MYSQL*)conn->get_mysql());
            }
            else
            {
                proxy_trace_flush_kafka(trace_node, &digest);
            }

            format_cache_node_t* tmp_node = trace_node;
            LIST_ADD_FIRST(link, flush_node->de_free_list, tmp_node);
            my_hash_delete(flush_node->de_trace_hash, (uchar*)tmp_node);
        }
    }

    flush_node->running = false;
    mysql_mutex_lock(&LOCK_thread_count);
    exe_thd->unlink();
    mysql_mutex_unlock(&LOCK_thread_count);
    close_backend_conn(conn);
    my_free(conn);
    str_deinit(&digest);
    delete exe_thd;
    my_thread_end();
    pthread_exit(0);
    return NULL;
}

pthread_handler_t proxy_sql_bucket_flush_thread(void* arg)
{
    THD *exe_thd= NULL;
    trace_sql_bucket_t* sql_bucket;
    struct timespec abstime;
    format_cache_node_t* bucket_node;
    str_t digest;
    str_init(&digest);
    backend_conn_t* conn;
    conn = (backend_conn_t*)my_malloc(sizeof(backend_conn_t), MY_ZEROFILL);
    my_thread_init();
    exe_thd = new THD(0);
    exe_thd->thread_stack= (char*) &exe_thd;
    
    sql_bucket = (trace_sql_bucket_t*)arg;
    setup_connection_thread_globals(exe_thd);
    int dequeue_index;
    int enqueue_index;
    
    sql_bucket->running = true;
    while (!abort_loop && trace_sql_inited)
    {
        dequeue_index = sql_bucket->bucket_dequeue_index;
        enqueue_index = sql_bucket->bucket_enqueue_index;
        
        if (dequeue_index != enqueue_index)
        {
            bucket_node = &sql_bucket->sql_bucket[dequeue_index];
            while(bucket_node->state == TRACE_NODE_NULL)
            {
                mysql_mutex_lock(&sql_bucket->bucket_lock);
                set_timespec_nsec(abstime, proxy_trace_flush_thread_sleep_msec * 1000000ULL);
                mysql_cond_timedwait(&sql_bucket->bucket_cond, &sql_bucket->bucket_lock, &abstime);
                mysql_mutex_unlock(&sql_bucket->bucket_lock);
            }
            if (global_trace_cache->trace_storage_type == PROXY_TRACE_STORAGE_MYSQL)
            {
                conn->set_mysql(proxy_get_trace_server_connection_with_con(conn));
                if (conn->get_mysql() == NULL)
                {
                    mysql_mutex_lock(&sql_bucket->bucket_lock);
                    set_timespec_nsec(abstime, proxy_trace_flush_thread_sleep_msec * 1000000ULL);
                    mysql_cond_timedwait(&sql_bucket->bucket_cond, 
                        &sql_bucket->bucket_lock, &abstime);
                    mysql_mutex_unlock(&sql_bucket->bucket_lock);
                    continue;
                }
                bucket_node->state = TRACE_NODE_FLUSH;
                proxy_trace_sql_flush(bucket_node, &digest, (MYSQL*)conn->get_mysql());
            }
            else
            {
                bucket_node->state = TRACE_NODE_FLUSH;
                proxy_trace_sql_flush_kafka(bucket_node, &digest);
            }

            bucket_node->state = TRACE_NODE_NULL;
            sql_bucket->bucket_dequeue_index = (dequeue_index + 1) % sql_bucket->bucket_size;
        }
        else
        {
            mysql_mutex_lock(&sql_bucket->bucket_lock);
            set_timespec_nsec(abstime, proxy_trace_flush_thread_sleep_msec * 1000000ULL);
            mysql_cond_timedwait(&sql_bucket->bucket_cond, &sql_bucket->bucket_lock, &abstime);
            mysql_mutex_unlock(&sql_bucket->bucket_lock);
        }
    }
    
    sql_bucket->running = false;
    mysql_mutex_lock(&LOCK_thread_count);
    exe_thd->unlink();
    mysql_mutex_unlock(&LOCK_thread_count);
    close_backend_conn(conn);
    my_free(conn);
    str_deinit(&digest);
    delete exe_thd;
    my_thread_end();
    pthread_exit(0);
    return NULL;
}
