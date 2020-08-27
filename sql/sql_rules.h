
#ifndef SQL_RULES
#define SQL_RULES

#include <vector>
#include <string>

#define RULE_TYPE_rule_id                   1
#define RULE_TYPE_active                    2
#define RULE_TYPE_timeout                   4 
#define RULE_TYPE_log                       8
#define RULE_TYPE_apply                     16
#define RULE_TYPE_negate_match_pattern      32
#define RULE_TYPE_proxy_port                64

#define RULE_TYPE_username                  512
#define RULE_TYPE_digest                    1024
#define RULE_TYPE_re_modifiers              2048
#define RULE_TYPE_replace_pattern           4096
#define RULE_TYPE_destination               8192
#define RULE_TYPE_comment                   16384
#define RULE_TYPE_schemaname                32768
#define RULE_TYPE_error_msg                 65536
#define RULE_TYPE_client_addr               131072
#define RULE_TYPE_proxy_addr                262144
#define RULE_TYPE_match_digest              524288
#define RULE_TYPE_match_pattern             1048576
#define RULE_TYPE_END                       0

class Query_Processor_Output {
  public:
  void *ptr;
  unsigned int size;
  char* destination_hostgroup;
  int destination;
  int mirror_hostgroup;
  int mirror_flagOUT;
  int next_query_flagIN;
  int cache_ttl;
  int reconnect;
  int timeout;
  int retries;
  int delay;
  char *error_msg;
  char *OK_msg;
  int sticky_conn;
  int multiplex;
  int log;
  char *comment; // #643
  std::string *new_query;
  void * operator new(size_t size) {
    return malloc(size);
  }
  void operator delete(void *ptr) {
    free(ptr);
  }
  Query_Processor_Output() {
    init();
  }
  ~Query_Processor_Output() {
    destroy();
  }
  void init() {
    ptr=NULL;
    size=0;
    destination_hostgroup=NULL;
    mirror_hostgroup=-1;
    mirror_flagOUT=-1;
    next_query_flagIN=-1;
    cache_ttl=-1;
    reconnect=-1;
    timeout=-1;
    retries=-1;
    delay=-1;
    sticky_conn=-1;
    multiplex=-1;
    log=-1;
    new_query=NULL;
    error_msg=NULL;
    OK_msg=NULL;
    comment=NULL; // #643
  }
  void destroy() {
    if (error_msg) {
      free(error_msg);
      error_msg=NULL;
    }
    if (OK_msg) {
      free(OK_msg);
      OK_msg=NULL;
    }
    if (comment) { // #643
      free(comment);
    }
    if (comment) { // #643
      free(comment);
    }
  }
};

typedef struct _Query_Processor_rule_t QP_rule_t;
struct _Query_Processor_rule_t {
  int rule_id;
  bool active;
  char *username;
  char *schemaname;
  int flagIN;
  char *client_addr;
  int client_addr_wildcard_position;
  char *proxy_addr;
  int proxy_port;
  uint64_t digest;
  char *digest_str;
  char *match_digest;
  char *match_pattern;
  bool negate_match_pattern;
  char *re_modifiers_str;
  int re_modifiers; // note: this is passed as char*, but converted to bitsfield
  int flagOUT;
  char *replace_pattern;
  char* destination_hostgroup;
  int destination;
  int cache_ttl;
  int reconnect;
  int timeout;
  int retries;
  int delay;
  int next_query_flagIN;
  int mirror_hostgroup;
  int mirror_flagOUT;
  char *error_msg;
  char *OK_msg;
  int sticky_conn;
  int multiplex;
  int log;
  bool apply;
  char *comment; // #643
  void *regex_engine1;
  void *regex_engine2;
  uint64_t hits;
  struct _Query_Processor_rule_t *parent; // pointer to parent, to speed up parent update
  uint64_t op_mask;
  LIST_NODE_T(QP_rule_t) link;
};


#define PROXYSQL_TOKENIZER_BUFFSIZE 128

#ifndef FIRST_COMMENT_MAX_LENGTH
#define FIRST_COMMENT_MAX_LENGTH  1024
#endif /* FIRST_COMMENT_MAX_LENGTH */

typedef struct
{
  char        buffer[PROXYSQL_TOKENIZER_BUFFSIZE];
  int         s_length;
  char*       s;
  const char* delimiters;
  char*       current;
  char*       next;
  int         is_ignore_empties;
}
tokenizer_t;

enum { TOKENIZER_EMPTIES_OK, TOKENIZER_NO_EMPTIES };

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */
//tokenizer_t tokenizer( const char* s, const char* delimiters, int empties );
void tokenizer( tokenizer_t *, const char* s, const char* delimiters, int empties );
const char* free_tokenizer( tokenizer_t* tokenizer );
const char* tokenize( tokenizer_t* tokenizer );
char * mysql_query_digest_and_first_comment(char *s , int len , char **first_comment, char *buf);
void c_split_2(const char *in, const char *del, char **out1, char **out2);
#ifdef __cplusplus
}
#endif /* __cplusplus */

QP_rule_t * new_query_rule(int rule_id, bool active, char *username, char *schemaname, int flagIN, char *client_addr, char *proxy_addr, int proxy_port, char *digest, char *match_digest, char *match_pattern, bool negate_match_pattern, char *re_modifiers, int flagOUT, char *replace_pattern, char* destination_hostgroup, int cache_ttl, int reconnect, int timeout, int retries, int delay, int next_query_flagIN, int mirror_flagOUT, int mirror_hostgroup, char *error_msg, char *OK_msg, int sticky_conn, int multiplex, int log, bool apply, char *comment);

int create_rule_if_null(THD* thd, char* rule_field, uint64_t intvalue, char* charvalue);
void delete_query_rule(QP_rule_t *qr);
void __reset_rules(std::vector<QP_rule_t *> * qrs);
int proxy_timeout_cache_init();
int proxy_assign_thd_timeout(THD* thd);
int proxy_release_thd_timeout(THD* thd);

#endif /* SQL_RULES */
