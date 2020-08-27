#include "sql_class.h"

int mysql_fetch_server_status(backend_conn_t* conn);
enum enum_proxy_trace_storage{
    PROXY_TRACE_STORAGE_MYSQL,
    PROXY_TRACE_STORAGE_KAFKA,
    PROXY_TRACE_STORAGE_NONE,
};

#define PRINT_MALLOC(info, type) sql_print_information("arkproxy malloc size: %d, msg: %s", sizeof(type), info)
