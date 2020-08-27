typedef char my_bool;
typedef int my_socket;
enum enum_server_command
{
  COM_SLEEP, COM_QUIT, COM_INIT_DB, COM_QUERY, COM_FIELD_LIST,
  COM_CREATE_DB, COM_DROP_DB, COM_REFRESH, COM_SHUTDOWN, COM_STATISTICS,
  COM_PROCESS_INFO, COM_CONNECT, COM_PROCESS_KILL, COM_DEBUG, COM_PING,
  COM_TIME, COM_DELAYED_INSERT, COM_CHANGE_USER, COM_BINLOG_DUMP,
  COM_TABLE_DUMP, COM_CONNECT_OUT, COM_REGISTER_SLAVE,
  COM_STMT_PREPARE, COM_STMT_EXECUTE, COM_STMT_SEND_LONG_DATA, COM_STMT_CLOSE,
  COM_STMT_RESET, COM_SET_OPTION, COM_STMT_FETCH, COM_DAEMON,
  COM_UNIMPLEMENTED,
  COM_RESET_CONNECTION,
  COM_MDB_GAP_BEG,
  COM_MDB_GAP_END=250,
  COM_SLAVE_WORKER=251,
  COM_SLAVE_IO=252,
  COM_SLAVE_SQL=253,
  COM_MULTI=254,
  COM_END=255
};
enum enum_indicator_type
{
  STMT_INDICATOR_NONE= 0,
  STMT_INDICATOR_NULL,
  STMT_INDICATOR_DEFAULT,
  STMT_INDICATOR_IGNORE
};
struct st_vio;
typedef struct st_vio Vio;
typedef struct st_net {
  Vio *vio;
  unsigned char *buff,*buff_end,*write_pos,*read_pos;
  my_socket fd;
  unsigned long remain_in_buf,length, buf_length, where_b;
  unsigned long max_packet,max_packet_size;
  unsigned int pkt_nr,compress_pkt_nr;
  unsigned int write_timeout, read_timeout, retry_count;
  int fcntl;
  unsigned int *return_status;
  unsigned char reading_or_writing;
  char save_char;
  char net_skip_rest_factor;
  my_bool thread_specific_malloc;
  unsigned char compress;
  my_bool unused3;
  void *thd;
  unsigned int last_errno;
  unsigned char error;
  my_bool unused4;
  my_bool unused5;
  char last_error[512];
  char sqlstate[5 +1];
  void *extension;
} NET;
enum enum_field_types { MYSQL_TYPE_DECIMAL, MYSQL_TYPE_TINY,
   MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG,
   MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE,
   MYSQL_TYPE_NULL, MYSQL_TYPE_TIMESTAMP,
   MYSQL_TYPE_LONGLONG,MYSQL_TYPE_INT24,
   MYSQL_TYPE_DATE, MYSQL_TYPE_TIME,
   MYSQL_TYPE_DATETIME, MYSQL_TYPE_YEAR,
   MYSQL_TYPE_NEWDATE, MYSQL_TYPE_VARCHAR,
   MYSQL_TYPE_BIT,
                        MYSQL_TYPE_TIMESTAMP2,
                        MYSQL_TYPE_DATETIME2,
                        MYSQL_TYPE_TIME2,
                        MYSQL_TYPE_NEWDECIMAL=246,
   MYSQL_TYPE_ENUM=247,
   MYSQL_TYPE_SET=248,
   MYSQL_TYPE_TINY_BLOB=249,
   MYSQL_TYPE_MEDIUM_BLOB=250,
   MYSQL_TYPE_LONG_BLOB=251,
   MYSQL_TYPE_BLOB=252,
   MYSQL_TYPE_VAR_STRING=253,
   MYSQL_TYPE_STRING=254,
   MYSQL_TYPE_GEOMETRY=255
};
enum mysql_enum_shutdown_level {
  SHUTDOWN_DEFAULT = 0,
  SHUTDOWN_WAIT_CONNECTIONS= (unsigned char)(1 << 0),
  SHUTDOWN_WAIT_TRANSACTIONS= (unsigned char)(1 << 1),
  SHUTDOWN_WAIT_UPDATES= (unsigned char)(1 << 3),
  SHUTDOWN_WAIT_ALL_BUFFERS= ((unsigned char)(1 << 3) << 1),
  SHUTDOWN_WAIT_CRITICAL_BUFFERS= ((unsigned char)(1 << 3) << 1) + 1
};
enum enum_cursor_type
{
  CURSOR_TYPE_NO_CURSOR= 0,
  CURSOR_TYPE_READ_ONLY= 1,
  CURSOR_TYPE_FOR_UPDATE= 2,
  CURSOR_TYPE_SCROLLABLE= 4
};
enum enum_mysql_set_option
{
  MYSQL_OPTION_MULTI_STATEMENTS_ON,
  MYSQL_OPTION_MULTI_STATEMENTS_OFF
};
enum enum_session_state_type
{
  SESSION_TRACK_SYSTEM_VARIABLES,
  SESSION_TRACK_SCHEMA,
  SESSION_TRACK_STATE_CHANGE,
  SESSION_TRACK_GTIDS,
  SESSION_TRACK_TRANSACTION_CHARACTERISTICS,
  SESSION_TRACK_TRANSACTION_STATE,
  SESSION_TRACK_always_at_the_end
};
my_bool my_net_init(NET *net, Vio* vio, void *thd, unsigned int my_flags);
void my_net_local_init(NET *net);
void net_end(NET *net);
void net_clear(NET *net, my_bool clear_buffer);
my_bool net_realloc(NET *net, size_t length);
my_bool net_flush(NET *net);
my_bool my_net_write(NET *net,const unsigned char *packet, size_t len);
my_bool net_write_command(NET *net,unsigned char command,
     const unsigned char *header, size_t head_len,
     const unsigned char *packet, size_t len);
int net_real_write(NET *net,const unsigned char *packet, size_t len);
unsigned long my_net_read_packet(NET *net, my_bool read_from_server);
unsigned long my_net_read_packet_reallen(NET *net, my_bool read_from_server,
                                         unsigned long* reallen);
struct sockaddr;
int my_connect(my_socket s, const struct sockaddr *name, unsigned int namelen,
        unsigned int timeout);
struct my_rnd_struct;
enum Item_result
{
  STRING_RESULT=0, REAL_RESULT, INT_RESULT, ROW_RESULT, DECIMAL_RESULT,
  TIME_RESULT
};
typedef struct st_udf_args
{
  unsigned int arg_count;
  enum Item_result *arg_type;
  char **args;
  unsigned long *lengths;
  char *maybe_null;
  char **attributes;
  unsigned long *attribute_lengths;
  void *extension;
} UDF_ARGS;
typedef struct st_udf_init
{
  my_bool maybe_null;
  unsigned int decimals;
  unsigned long max_length;
  char *ptr;
  my_bool const_item;
  void *extension;
} UDF_INIT;
void create_random_string(char *to, unsigned int length,
                          struct my_rnd_struct *rand_st);
my_bool caculate_first_stage_hash1(const uchar *scramble_arg, const char *message,
               const uint8 *hash_stage1, const uint8 *hash_stage2);
void hash_password(unsigned long *to, const char *password, unsigned int password_len);
void make_scrambled_password_323(char *to, const char *password);
void scramble_323(char *to, const char *message, const char *password);
my_bool check_scramble_323(const unsigned char *reply, const char *message,
                           unsigned long *salt);
void get_salt_from_password_323(unsigned long *res, const char *password);
void make_scrambled_password(char *to, const char *password);
void scramble(char *to, const char *message, const char *password);
my_bool check_scramble(const unsigned char *reply, const char *message,
                       const unsigned char *hash_stage2);
void get_salt_from_password(unsigned char *res, const char *password);
char *octet2hex(char *to, const char *str, unsigned int len);
void scramble_replace_scramble( char *to, const char *new_scramble, const char *hash_stage_2_base) ;
char *get_tty_password(const char *opt_message);
void get_tty_password_buff(const char *opt_message, char *to, size_t length);
const char *mysql_errno_to_sqlstate(unsigned int mysql_errno);
my_bool my_thread_init(void);
void my_thread_end(void);
enum net_state{
    NET_STATE_ZERO,
    NET_STATE_META_EOF,
    NET_STATE_EOF,
    NET_STATE_FULL,
    NET_STATE_NULL,
    NET_STATE_ERROR,
    NET_STATE_OK,
    NET_STATE_GEN,
    NET_STATE_FULL_END,
    NET_STATE_EMPTY,
    NET_STATE_META_EOF_END,
    NET_STATE_SKIP,
    NET_STATE_EOF_NONEND,
    NET_STATE_END_FLUSH,
};
enum trace_queue_state{
    TRACE_STATE_EMPTY,
    TRACE_STATE_BUSY,
    TRACE_STATE_FORMAT,
    TRACE_STATE_RESET,
};
enum trace_node_state{
    TRACE_NODE_FULL,
    TRACE_NODE_NULL,
    TRACE_NODE_FLUSH,
};
enum trace_thd_state{
    THD_RUNNING,
    THD_END,
    THD_NULL,
};
enum trace_busy_strategy{
    BUSY_SKIP,
    BUSY_WAIT,
};
typedef long my_time_t;
enum enum_mysql_timestamp_type
{
  MYSQL_TIMESTAMP_NONE= -2, MYSQL_TIMESTAMP_ERROR= -1,
  MYSQL_TIMESTAMP_DATE= 0, MYSQL_TIMESTAMP_DATETIME= 1, MYSQL_TIMESTAMP_TIME= 2
};
typedef struct st_mysql_time
{
  unsigned int year, month, day, hour, minute, second;
  unsigned long second_part;
  my_bool neg;
  enum enum_mysql_timestamp_type time_type;
} MYSQL_TIME;
