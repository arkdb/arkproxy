#include "sql_class.h"

int proxy_format_to_queue_before(THD* thd);
int proxy_format_to_queue_after(THD* thd, ulonglong exe_time, ulonglong affect_rows);
unsigned long long md5_16_int(char* source);
int proxy_trace_cache_init();
int proxy_sql_trace_init();
int proxy_trace_cache_deinit();
int proxy_sql_trace_deinit();
int proxy_trace_version();
int proxy_trace_digest_check_empty();
int proxy_sql_trace_check_empty();
int proxy_digest_cache_on();
