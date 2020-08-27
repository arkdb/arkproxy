#include "sql_class.h"
#include "sql_rules.h"
#include "pcrecpp.h"
#include "pcrecpp.cc"
#include "sql_acl.h"
#include "sql_connect.h"
#include "mysql.h"

#define QP_RE_MOD_CASELESS 1
#define QP_RE_MOD_GLOBAL 2

timeout_cache_t global_timeout_cache_space;
timeout_cache_t* global_timeout_cache = &global_timeout_cache_space;
extern MYSQL* get_backend_connection(THD* thd, backend_conn_t* conn);

void tokenizer(tokenizer_t *result, const char* s, const char* delimiters, int empties )
{
    //tokenizer_t result;

    result->s_length = ( (s && delimiters) ? strlen(s) : 0 );
    result->s = NULL;
    if (result->s_length) {
        if (result->s_length > (PROXYSQL_TOKENIZER_BUFFSIZE-1)) {
            result->s = strdup(s);
        } else {
            strcpy(result->buffer,s);
            result->s = result->buffer;
        }
    }
    result->delimiters              = delimiters;
    result->current                  = NULL;
    result->next                            = result->s;
    result->is_ignore_empties = (empties != TOKENIZER_EMPTIES_OK);

    //return result;
}

const char* free_tokenizer( tokenizer_t* tokenizer )
{
    if (tokenizer->s_length > (PROXYSQL_TOKENIZER_BUFFSIZE-1)) {
        free(tokenizer->s);
    }
    tokenizer->s = NULL;
    return NULL;
}

const char* tokenize( tokenizer_t* tokenizer )
{
    if (!tokenizer->s) return NULL;

    if (!tokenizer->next)
        return free_tokenizer( tokenizer );

    tokenizer->current = tokenizer->next;
    tokenizer->next = strpbrk( tokenizer->current, tokenizer->delimiters );

    if (tokenizer->next)
    {
        *tokenizer->next = '\0';
        tokenizer->next += 1;

        if (tokenizer->is_ignore_empties)
        {
            tokenizer->next += strspn( tokenizer->next, tokenizer->delimiters );
            if (!(*tokenizer->current))
                return tokenize( tokenizer );
        }
    }
    else if (tokenizer->is_ignore_empties && !(*tokenizer->current))
        return free_tokenizer( tokenizer );

    return tokenizer->current;
}

struct __RE2_objects_t {
    pcrecpp::RE_Options *opt1;
    pcrecpp::RE *re1;
    //re2::RE2::Options *opt2;
    //RE2 *re2;
};

typedef struct __RE2_objects_t re2_t;

static re2_t * compile_query_rule(QP_rule_t *qr, int i) {
    re2_t *r=(re2_t *)malloc(sizeof(re2_t));
    r->opt1=NULL;
    r->re1=NULL;
    //r->opt2=NULL;
    //r->re2=NULL;
    //if (mysql_thread___query_processor_regex==2) {
    //    r->opt2=new re2::RE2::Options(RE2::Quiet);
    //    if ((qr->re_modifiers & QP_RE_MOD_CASELESS) == QP_RE_MOD_CASELESS) {
    //        r->opt2->set_case_sensitive(false);
    //    }
    //    if (i==1) {
    //        r->re2=new RE2(qr->match_digest, *r->opt2);
    //    } else if (i==2) {
    //        r->re2=new RE2(qr->match_pattern, *r->opt2);
    //    }
    //} else 
    {
        r->opt1=new pcrecpp::RE_Options();
        if ((qr->re_modifiers & QP_RE_MOD_CASELESS) == QP_RE_MOD_CASELESS) {
            r->opt1->set_caseless(true);
        }
        if (i==1) {
            r->re1=new pcrecpp::RE(qr->match_digest, *r->opt1);
        } else if (i==2) {
            r->re1=new pcrecpp::RE(qr->match_pattern, *r->opt1);
        }
    }
    return r;
};

void delete_query_rule(QP_rule_t *qr) {
    if (qr->username)
        free(qr->username);
    if (qr->schemaname)
        free(qr->schemaname);
    if (qr->match_pattern)
        free(qr->match_pattern);
    if (qr->replace_pattern)
        free(qr->replace_pattern);
    if (qr->error_msg)
        free(qr->error_msg);
    if (qr->OK_msg)
        free(qr->OK_msg);
    if (qr->regex_engine1) {
        re2_t *r=(re2_t *)qr->regex_engine1;
        if (r->opt1) { delete r->opt1; r->opt1=NULL; }
        if (r->re1) { delete r->re1; r->re1=NULL; }
        //if (r->opt2) { delete r->opt2; r->opt2=NULL; }
        //if (r->re2) { delete r->re2; r->re2=NULL; }
        free(qr->regex_engine1);
    }
    if (qr->regex_engine2) {
        re2_t *r=(re2_t *)qr->regex_engine2;
        if (r->opt1) { delete r->opt1; r->opt1=NULL; }
        if (r->re1) { delete r->re1; r->re1=NULL; }
        //if (r->opt2) { delete r->opt2; r->opt2=NULL; }
        //if (r->re2) { delete r->re2; r->re2=NULL; }
        free(qr->regex_engine2);
    }
    my_free(qr);
}

// delete all the query rules in a Query Processor Table
// Note that this function is called by GloQPro with &rules (generic table)
//     and is called by each mysql thread with _thr_SQP_rules (per thread table)
void __reset_rules(std::vector<QP_rule_t *> * qrs) {
    if (qrs==NULL) return;
    QP_rule_t *qr;
    for (std::vector<QP_rule_t *>::iterator it=qrs->begin(); it!=qrs->end(); ++it) {
        qr=*it;
        delete_query_rule(qr);
    }
    qrs->clear();
}

QP_rule_t * new_query_rule(int rule_id, bool active, char *username, char *schemaname, int flagIN, char *client_addr, char *proxy_addr, int proxy_port, char *digest, char *match_digest, char *match_pattern, bool negate_match_pattern, char *re_modifiers, int flagOUT, char *replace_pattern, char* destination_hostgroup, int cache_ttl, int reconnect, int timeout, int retries, int delay, int next_query_flagIN, int mirror_flagOUT, int mirror_hostgroup, char *error_msg, char *OK_msg, int sticky_conn, int multiplex, int log, bool apply, char *comment) {
    QP_rule_t * newQR=(QP_rule_t *)my_malloc(sizeof(QP_rule_t), MY_ZEROFILL);
    newQR->rule_id=rule_id;
    newQR->active=active;
    newQR->username=(username ? strdup(username) : NULL);
    newQR->schemaname=(schemaname ? strdup(schemaname) : NULL);
    newQR->flagIN=flagIN;
    newQR->match_digest=(match_digest ? strdup(match_digest) : NULL);
    newQR->match_pattern=(match_pattern ? strdup(match_pattern) : NULL);
    newQR->negate_match_pattern=negate_match_pattern;
    newQR->re_modifiers_str =(re_modifiers ? strdup(re_modifiers) : NULL);
    newQR->re_modifiers=0;
    if (re_modifiers)
    {
        tokenizer_t tok;
        tokenizer( &tok, re_modifiers, ",", TOKENIZER_NO_EMPTIES );
        const char* token;
        for (token = tokenize( &tok ); token; token = tokenize( &tok )) {
            if (strncasecmp(token,(char *)"CASELESS",strlen((char *)"CASELESS"))==0) {
                newQR->re_modifiers|=QP_RE_MOD_CASELESS;
            }
            if (strncasecmp(token,(char *)"GLOBAL",strlen((char *)"GLOBAL"))==0) {
                newQR->re_modifiers|=QP_RE_MOD_GLOBAL;
            }
        }
        free_tokenizer( &tok );
    }
    newQR->flagOUT=flagOUT;
    newQR->replace_pattern=(replace_pattern ? strdup(replace_pattern) : NULL);
    newQR->destination_hostgroup =(destination_hostgroup ? strdup(destination_hostgroup) : NULL);
    if (destination_hostgroup && 
        strcasecmp(destination_hostgroup,(char *)"WRITE") &&
        strcasecmp(destination_hostgroup,(char *)"READ"))
    {
        sql_print_error("Incorrect destination for rule_id %d : %s\n" , 
            rule_id, destination_hostgroup);
        return NULL;
    }

    if (destination_hostgroup)
    {
        newQR->destination=
          strcasecmp(destination_hostgroup,(char *)"WRITE") ? ROUTER_TYPE_RO : ROUTER_TYPE_RW;
    }

    newQR->cache_ttl=cache_ttl;
    newQR->reconnect=reconnect;
    newQR->timeout=timeout;
    newQR->retries=retries;
    newQR->delay=delay;
    newQR->next_query_flagIN=next_query_flagIN;
    newQR->mirror_flagOUT=mirror_flagOUT;
    newQR->mirror_hostgroup=mirror_hostgroup;
    newQR->error_msg=(error_msg ? strdup(error_msg) : NULL);
    newQR->OK_msg=(OK_msg ? strdup(OK_msg) : NULL);
    newQR->sticky_conn=sticky_conn;
    newQR->multiplex=multiplex;
    newQR->apply=apply;
    newQR->comment=(comment ? strdup(comment) : NULL); // see issue #643
    newQR->regex_engine1=NULL;
    newQR->regex_engine2=NULL;
    newQR->hits=0;

    //newQR->client_addr_wildcard_position = -1; // not existing by default
    newQR->client_addr=(client_addr ? strdup(client_addr) : NULL);
    //if (newQR->client_addr) {
    //    char *pct = strchr(newQR->client_addr,'%');
    //    if (pct) { // there is a wildcard . We assume Admin did already all the input validation
    //        if (pct == newQR->client_addr) {
    //            // client_addr == '%'
    //            // % is at the end of the string, but also at the beginning
    //            // becoming a catch all
    //            newQR->client_addr_wildcard_position = 0;
    //        } else {
    //            // this math is valid also if (pct == newQR->client_addr)
    //            // but we separate it to clarify that client_addr_wildcard_position is a match all
    //            newQR->client_addr_wildcard_position = strlen(newQR->client_addr) - strlen(pct);
    //        }
    //    }
    //}
    newQR->proxy_addr=(proxy_addr ? strdup(proxy_addr) : NULL);
    newQR->proxy_port=proxy_port;
    newQR->log=log;
    newQR->digest=0;
    newQR->digest_str =(digest ? strdup(digest) : NULL);
    if (digest) {
        unsigned long long num=strtoull(digest,NULL,0);
        if (num!=ULLONG_MAX && num!=0) {
            newQR->digest=num;
        } else {
            sql_print_error("Incorrect digest for rule_id %d : %s\n" , rule_id, digest);
            return NULL;
        }
    }
    return newQR;
};

Query_Processor_Output * process_mysql_query(THD* thd) {
    query_rules_t* query_rule;
    if (thd->query_rule->qpo == NULL)
        thd->query_rule->qpo = (Query_Processor_Output*)my_malloc(
            sizeof(Query_Processor_Output), MY_ZEROFILL);

    if (thd->query_rule->_thr_SQP_rules == NULL)
        thd->query_rule->_thr_SQP_rules=new std::vector<QP_rule_t *>;

    if (!thd->query_rule || !thd->query_rule->qpo || thd->get_command() != COM_QUERY)
        return NULL;

    query_rule = thd->query_rule;
    // to avoid unnecssary deallocation/allocation, we initialize qpo witout new allocation
    Query_Processor_Output *ret = query_rule->qpo;
    ret->init();
#define stackbuffer_size 128
    char stackbuffer[stackbuffer_size];
    unsigned int len=thd->query_length();
    char *query=NULL;
    if (len < stackbuffer_size) {
        query=stackbuffer;
    } else {
        query=(char *)my_malloc(len+1, MYF(0));
    }
    memcpy(query,(char *)thd->query(),thd->query_length());
    query[len]=0;
    Security_context *sctx= thd->security_ctx;
    if (__sync_add_and_fetch(&global_proxy_config.rules_version,0) > query_rule->rule_version) {
        // update local rules;
        mysql_mutex_lock(&global_proxy_config.config_lock);
        query_rule->rule_version = __sync_add_and_fetch(&global_proxy_config.rules_version, 0);
        __reset_rules(query_rule->_thr_SQP_rules);
        QP_rule_t *qr1;
        QP_rule_t *qr2;
        for (std::vector<QP_rule_t *>::iterator it=global_proxy_config.rules.begin(); 
            it!=global_proxy_config.rules.end(); ++it) 
        {
            qr1=*it;
            if (qr1->active) 
            {
                char buf[20];
                if (qr1->digest) { // not 0
                    sprintf(buf,"0x%016llX", (long long unsigned int)qr1->digest);
                }
                std::string re_mod;
                re_mod="";
                if ((qr1->re_modifiers & QP_RE_MOD_CASELESS) == QP_RE_MOD_CASELESS) 
                  re_mod = "CASELESS";
                if ((qr1->re_modifiers & QP_RE_MOD_GLOBAL) == QP_RE_MOD_GLOBAL) 
                {
                    if (re_mod.length()) 
                    {
                        re_mod = re_mod + ",";
                    }
                    re_mod = re_mod + "GLOBAL";
                }
                qr2=new_query_rule(qr1->rule_id, qr1->active, 
                    qr1->username, qr1->schemaname, qr1->flagIN,
                    qr1->client_addr, qr1->proxy_addr, qr1->proxy_port,
                    ( qr1->digest ? buf : NULL ) ,
                    qr1->match_digest, qr1->match_pattern, 
                    qr1->negate_match_pattern, (char *)re_mod.c_str(),
                    qr1->flagOUT, qr1->replace_pattern, qr1->destination_hostgroup,
                    qr1->cache_ttl, qr1->reconnect, qr1->timeout, qr1->retries, 
                    qr1->delay, qr1->next_query_flagIN, qr1->mirror_flagOUT, qr1->mirror_hostgroup,
                    qr1->error_msg, qr1->OK_msg, qr1->sticky_conn, 
                    qr1->multiplex, qr1->log, qr1->apply, qr1->comment);
                if (!qr2)
                    continue;
                qr2->parent=qr1;    // pointer to parent to speed up parent update (hits)
                if (qr2->match_digest) {
                    qr2->regex_engine1=(void *)compile_query_rule(qr2,1);
                }
                if (qr2->match_pattern) {
                    qr2->regex_engine2=(void *)compile_query_rule(qr2,2);
                }
                query_rule->_thr_SQP_rules->push_back(qr2);
            }
        }
        //delete _thr_SQP_rules_fast_routing;
        //_thr_SQP_rules_fast_routing = new std::unordered_map<std::string, int>(rules_fast_routing);
        //for (std::unordered_map<std::string, int>::iterator it = rules_fast_routing.begin(); it != rules_fast_routing.end(); ++it) {
        //    _thr_SQP_rules_fast_routing->insert(
        //}
        mysql_mutex_unlock(&global_proxy_config.config_lock);
    }
    QP_rule_t *qr = NULL;
    re2_t *re2p;
    int flagIN=0;
    ret->next_query_flagIN=-1; // reset
    //if (sess->next_query_flagIN >= 0) {
    //    flagIN=sess->next_query_flagIN;
    //}
    //int reiterate=mysql_thread___query_processor_iterations;
    //if (sess->mirror==true) {
    //    // we are into a mirror session
    //    // we immediately set a destination_hostgroup
    //    ret->destination_hostgroup=sess->mirror_hostgroup;
    //    if (sess->mirror_flagOUT != -1) {
    //        // the original session has set a mirror flagOUT
    //        flagIN=sess->mirror_flagOUT;
    //    } else {
    //        // the original session did NOT set any mirror flagOUT
    //        // so we exit here
    //        // the only thing set so far is destination_hostgroup
    //        goto __exit_process_mysql_query;
    //    }
    //}
__internal_loop:
    for (std::vector<QP_rule_t *>::iterator it=query_rule->_thr_SQP_rules->begin(); 
        it!=query_rule->_thr_SQP_rules->end(); ++it) 
    {
        qr=*it;
        if (qr->flagIN != flagIN) {
            continue;
        }
        if (qr->username && strlen(qr->username)) {
            if (!sctx->user || strcmp(qr->username,sctx->user)!=0) {
                continue;
            }
        }
        if (qr->schemaname && strlen(qr->schemaname)) {
            if (!thd->db || strcmp(qr->schemaname,thd->db)!=0) {
                continue;
            }
        }

        // match on client address
        if (qr->client_addr && strlen(qr->client_addr)) {
            if (sctx->host_or_ip) {
                acl_host_and_ip host_and_ip;
                update_hostname(&host_and_ip, qr->client_addr);
                if (!compare_hostname(&host_and_ip, NULL, sctx->host_or_ip)){
                    continue;
                }
                //if (qr->client_addr_wildcard_position == -1) { // no wildcard , old algorithm
                //    if (strcmp(qr->client_addr,sctx->host_or_ip)!=0) {
                //        continue;
                //    }
                //} else if (qr->client_addr_wildcard_position==0) {
                //    // catch all!
                //    // therefore we have a match
                //} else { // client_addr_wildcard_position > 0
                //    if (strncmp(qr->client_addr,sctx->host_or_ip,
                //          qr->client_addr_wildcard_position)!=0) {
                //        continue;
                //    }
                //}
            }
        }

        // match on proxy_addr
        if (qr->proxy_addr && strlen(qr->proxy_addr)) {
            acl_host_and_ip host_and_ip;
            update_hostname(&host_and_ip, qr->proxy_addr);
            if (!compare_hostname(&host_and_ip, NULL, proxy_local_ip)){
                continue;
            }
        }

        // match on proxy_port
        if (qr->proxy_port > 0) {
            if (qr->proxy_port!= (int)mysqld_port) {
                continue;
            }
        }

        // match on digest
        if (query_rule->digest) {
            if (qr->digest) {
                if (qr->digest != query_rule->digest) {
                    continue;
                }
            }
        }

        // match on query digest
        if (query_rule->digest_text ) 
        { // we call this only if we have a query digest
            re2p=(re2_t *)qr->regex_engine1;
            if (qr->match_digest) {
                bool rc;
                // we always match on original query
                //if (re2p->re2) {
                //    rc=RE2::PartialMatch(query_rule->digest_text,*re2p->re2);
                //} else 
                {
                    rc=re2p->re1->PartialMatch(query_rule->digest_text);
                }
                if ((rc==true && qr->negate_match_pattern==true) || 
                    (rc==false && qr->negate_match_pattern==false )) 
                {
                    continue;
                }
            }
        }
        // match on query
        re2p=(re2_t *)qr->regex_engine2;
        if (qr->match_pattern) {
            bool rc;
            if (ret && ret->new_query) 
            {
                // if we already rewrote the query, process the new query
                //std::string *s=ret->new_query;
                //if (re2p->re2) {
                //    rc=RE2::PartialMatch(ret->new_query->c_str(),*re2p->re2);
                //} else 
                {
                    rc=re2p->re1->PartialMatch(ret->new_query->c_str());
                }
            } 
            else 
            {
                // we never rewrote the query
                //if (re2p->re2) {
                //    rc=RE2::PartialMatch(query,*re2p->re2);
                //} else 
                {
                    rc=re2p->re1->PartialMatch(query);
                }
            }

            if ((rc==true && qr->negate_match_pattern==true) || 
                ( rc==false && qr->negate_match_pattern==false )) {
                continue;
            }
        }

        // if we arrived here, we have a match
        // this is done without atomic function because it updates only the local variables
        
        qr->hits++; 
        bool set_flagOUT=false;
        if (qr->flagOUT >= 0) 
        {
            flagIN=qr->flagOUT;
            set_flagOUT=true;
        }
        if (qr->reconnect >= 0) {
                // Note: negative reconnect means this rule doesn't change
          ret->reconnect=qr->reconnect;
        }
        if (qr->timeout >= 0) {
                // Note: negative timeout means this rule doesn't change
          ret->timeout=qr->timeout;
        }
        if (qr->retries >= 0) {
                // Note: negative retries means this rule doesn't change
          ret->retries=qr->retries;
        }
        if (qr->delay >= 0) {
                // Note: negative delay means this rule doesn't change
          ret->delay=qr->delay;
        }
        if (qr->next_query_flagIN >= 0) {
          ret->mirror_flagOUT=qr->mirror_flagOUT;
        }
        if (qr->mirror_hostgroup >= 0) {
                // Note: negative mirror_hostgroup means this rule doesn't change the mirror
          ret->mirror_hostgroup=qr->mirror_hostgroup;
        }
        if (qr->error_msg) {
          ret->error_msg=strdup(qr->error_msg);
        }
        if (qr->OK_msg) {
          ret->OK_msg=strdup(qr->OK_msg);
        }
        if (qr->cache_ttl >= 0) {
          ret->cache_ttl=qr->cache_ttl;
        }
        if (qr->sticky_conn >= 0) {
          ret->sticky_conn=qr->sticky_conn;
        }
        if (qr->multiplex >= 0) {
          ret->multiplex=qr->multiplex;
        }
        if (qr->log >= 0) {
          ret->log=qr->log;
        }
        if (qr->destination_hostgroup) {
          ret->destination_hostgroup = strdup(qr->destination_hostgroup ) ;
        }
        if (qr->destination) {
          ret->destination= qr->destination;
        }

        if (qr->replace_pattern && qr->match_pattern) {
            if (ret->new_query==NULL) ret->new_query=new std::string(query);
            re2_t *re2p=(re2_t *)qr->regex_engine2;
            //if (re2p->re2) {
            //    //RE2::Replace(ret->new_query,qr->match_pattern,qr->replace_pattern);
            //    if ((qr->re_modifiers & QP_RE_MOD_GLOBAL) == QP_RE_MOD_GLOBAL) {
            //        re2p->re2->GlobalReplace(ret->new_query,qr->match_pattern,qr->replace_pattern);
            //    } else {
            //        re2p->re2->Replace(ret->new_query,qr->match_pattern,qr->replace_pattern);
            //    }
            //} else 
            {
                //re2p->re1->Replace(ret->new_query,qr->replace_pattern);
                if ((qr->re_modifiers & QP_RE_MOD_GLOBAL) == QP_RE_MOD_GLOBAL) {
                    re2p->re1->GlobalReplace(qr->replace_pattern,ret->new_query);
                } else {
                    re2p->re1->Replace(qr->replace_pattern,ret->new_query);
                }
            }
        }

        if (qr->apply==true) {
            goto __exit_process_mysql_query;
        }

        if (set_flagOUT==true) {
            //if (reiterate) {
            //    reiterate--;
            //    goto __internal_loop;
            //}
        }
    }

__exit_process_mysql_query:
    if (qr == NULL || qr->apply == false) {
        // now it is time to check mysql_query_rules_fast_routing
        // it is only check if "apply" is not true
        //size_t mapl = _thr_SQP_rules_fast_routing->size();
        //if (mapl) { // trigger new routing algorithm only if rules exists
        //    string s = sctx->user;
        //    s.append(rand_del);
        //    s.append(thd->db);
        //    s.append("---");
        //    s.append(std::to_string(flagIN));
        //    std::unordered_map<std::string, int>:: iterator it;
        //    it = _thr_SQP_rules_fast_routing->find(s);
        //    if (it != _thr_SQP_rules_fast_routing->end()) {
        //        ret->destination_hostgroup = it->second;
        //    }
        //}
    }
    // FIXME : there is too much data being copied around
    if (len < stackbuffer_size) {
        // query is in the stack
    } else {
        my_free(query);
    }
    //if (sess->mirror==false) { // we process comments only on original queries, not on mirrors
    //    if (qp && qp->first_comment) {
    //        // we have a comment to parse
    //        query_parser_first_comment(ret, qp->first_comment);
    //    }
    //}
    return ret;
};

int get_rule_type(char* rule_field)
{
    if (!strcasecmp(rule_field, "rule_id"))
        return RULE_TYPE_rule_id;
    if (!strcasecmp(rule_field, "active"))
        return RULE_TYPE_active;
    if (!strcasecmp(rule_field, "username"))
        return RULE_TYPE_username;
    if (!strcasecmp(rule_field, "schemaname"))
        return RULE_TYPE_schemaname;
    if (!strcasecmp(rule_field, "client_addr"))
        return RULE_TYPE_client_addr;
    if (!strcasecmp(rule_field, "proxy_addr"))
        return RULE_TYPE_proxy_addr;
    if (!strcasecmp(rule_field, "proxy_port"))
        return RULE_TYPE_proxy_port;
    if (!strcasecmp(rule_field, "digest"))
        return RULE_TYPE_digest;
    if (!strcasecmp(rule_field, "match_digest"))
        return RULE_TYPE_match_digest;
    if (!strcasecmp(rule_field, "match_pattern"))
        return RULE_TYPE_match_pattern;
    if (!strcasecmp(rule_field, "negate_match_pattern"))
        return RULE_TYPE_negate_match_pattern;
    if (!strcasecmp(rule_field, "re_modifiers"))
        return RULE_TYPE_re_modifiers;
    if (!strcasecmp(rule_field, "replace_pattern"))
        return RULE_TYPE_replace_pattern;
    if (!strcasecmp(rule_field, "destination"))
        return RULE_TYPE_destination;
    if (!strcasecmp(rule_field, "comment"))
        return RULE_TYPE_comment;
    if (!strcasecmp(rule_field, "timeout"))
        return RULE_TYPE_timeout;
    if (!strcasecmp(rule_field, "error_msg"))
        return RULE_TYPE_error_msg;
    if (!strcasecmp(rule_field, "log"))
        return RULE_TYPE_log;
    if (!strcasecmp(rule_field, "apply"))
        return RULE_TYPE_apply;
    return 0;
}

int create_rule_if_null(THD* thd, char* rule_field, uint64_t intvalue, char* charvalue)
{
    QP_rule_t* rule = NULL;
    int type;
    char tmp[1024];

    type = get_rule_type(rule_field);
    if (type == 0)
    {
        sprintf(tmp, "rule config field \'%s\' error", rule_field);
        my_error(ER_CONFIG_ERROR, MYF(0), tmp);
        return true;
    }

    /* set the field to NULL */
    if (intvalue == LLONG_MAX && charvalue == NULL)
    {

    }
    else if ((type < RULE_TYPE_username && intvalue == LLONG_MAX) ||
        (type >= RULE_TYPE_username && charvalue == NULL))
    {
        sprintf(tmp, "rule config field \'%s\' value type error", rule_field);
        my_error(ER_CONFIG_ERROR, MYF(0), tmp);
        return true;
    }

    if (!thd->config_ele)
    {
        thd->config_ele = (config_element_t*)my_malloc(sizeof(config_element_t), MY_ZEROFILL);
        rule = (QP_rule_t*)my_malloc(sizeof(QP_rule_t), MY_ZEROFILL);
        thd->config_ele->rule = rule;
        thd->config_ele->sub_command = thd->lex->sub_command;
    }

    rule = thd->config_ele->rule;
    switch (type)
    {
    case RULE_TYPE_rule_id:
        rule->rule_id = (int)intvalue;
        break;
    case RULE_TYPE_active:
        rule->active = (int)intvalue;
        break;
    case RULE_TYPE_username:
        rule->username = (charvalue ? strdup(charvalue) : NULL);
        break;
    case RULE_TYPE_schemaname:
        rule->schemaname = (charvalue ? strdup(charvalue) : NULL);
        break;
    case RULE_TYPE_client_addr:
        rule->client_addr = (charvalue ? strdup(charvalue) : NULL);
        break;
    case RULE_TYPE_proxy_addr:
        rule->proxy_addr = (charvalue ? strdup(charvalue) : NULL);
        break;
    case RULE_TYPE_proxy_port:
        rule->proxy_port = (int)intvalue;
        break;
    case RULE_TYPE_digest:
        rule->digest_str= (charvalue ? strdup(charvalue) : NULL);
        break;
    case RULE_TYPE_match_digest:
        rule->match_digest = (charvalue ? strdup(charvalue) : NULL);
        break;
    case RULE_TYPE_match_pattern:
        rule->match_pattern = (charvalue ? strdup(charvalue) : NULL);
        break;
    case RULE_TYPE_negate_match_pattern:
        rule->negate_match_pattern = (int)intvalue;
        break;
    case RULE_TYPE_re_modifiers:
        rule->re_modifiers_str = (charvalue ? strdup(charvalue) : NULL);
        break;
    case RULE_TYPE_replace_pattern:
        rule->replace_pattern = (charvalue ? strdup(charvalue) : NULL);
        break;
    case RULE_TYPE_destination:
        rule->destination_hostgroup = (charvalue ? strdup(charvalue) : NULL);
        break;
    case RULE_TYPE_comment:
        rule->comment = (charvalue ? strdup(charvalue) : NULL);
        break;
    case RULE_TYPE_timeout:
        rule->timeout = (int)intvalue;
        break;
    case RULE_TYPE_error_msg:
        rule->error_msg = (charvalue ? strdup(charvalue) : NULL);
        break;
    case RULE_TYPE_log:
        rule->log = (int)intvalue;
        break;
    case RULE_TYPE_apply:
        rule->apply = (int)intvalue;
        break;
 
    default:
        break;
    }

    /* record the operation types */
    rule->op_mask |= type;
    return false;
}

pthread_handler_t proxy_query_timeout_thread(void* arg)
{
    timedout_t * timeout;
    backend_conn_t conn;
    THD* thd;
    char sql[256];
    MYSQL* mysql;
    int64_t now_time;

    my_thread_init();
    thd = new THD(0);
    thd->thread_stack= (char*) &thd;
    
    setup_connection_thread_globals(thd);

    while (!abort_loop)
    {
        for (int i=0; i < global_timeout_cache->timeout_cache_size; i++)
        {
            timeout = &global_timeout_cache->timeout_queue[i];
            now_time = my_hrtime().val;
            if (timeout->timeout > 0 && timeout->start_time && 
                timeout->server && now_time - timeout->start_time > timeout->timeout * 1000000)
            {
                init_backend_conn_info(&conn, timeout->server->backend_host, backend_user,
                    backend_passwd, timeout->server->backend_port);
                mysql = get_backend_connection(thd, &conn);
                sprintf(sql, "KILL query %lld", timeout->thread_id);
                if (mysql_real_query(mysql, sql, strlen(sql)))
                {
                    sql_print_information("timeout query kill failed, timeout: %d, "
                        "thread_id: %d, msg: %s",
                        timeout->timeout, timeout->thread_id, mysql_error(mysql));
                }

                close_backend_connection(&conn);
            }
        }
        sleep(1);
    }

    delete thd;
    my_thread_end();
    pthread_exit(0);
}

int proxy_timeout_cache_init()
{
    timeout_cache_t* timeout_cache;
    timedout_t * timeout;
    pthread_t threadid;

    timeout_cache = global_timeout_cache;
    /* set to the value equal to max connection*/
    timeout_cache->timeout_cache_size = 100000;
    timeout_cache->timeout_queue = (timedout_t*)my_malloc(sizeof(timedout_t) * 
          timeout_cache->timeout_cache_size, MY_ZEROFILL);

    LIST_INIT(timeout_cache->free_list);
    LIST_INIT(timeout_cache->full_list);

    for (int i = 0; i < timeout_cache->timeout_cache_size; i++)
    {
        timeout = &timeout_cache->timeout_queue[i];
        LIST_ADD_LAST(link, timeout_cache->free_list, timeout);
    }
    
    mysql_thread_create(0, &threadid, &connection_attrib,
        proxy_query_timeout_thread, NULL);
    return false;
}

int proxy_assign_thd_timeout(THD* thd)
{
    if (!thd->query_rule || thd->connection_type != PROXY_CONNECT_PROXY)
        return false;

    timedout_t* timeout = NULL;
    mysql_mutex_lock(&global_timeout_cache->timeout_lock);

    timeout = LIST_GET_FIRST(global_timeout_cache->free_list);
    if (timeout == NULL)
    {
        mysql_mutex_unlock(&global_timeout_cache->timeout_lock);
        return true;
    }

    timeout->timeout = 0;
    timeout->start_time = 0;
    LIST_REMOVE(link, global_timeout_cache->free_list, timeout);
    LIST_ADD_FIRST(link, global_timeout_cache->full_list, timeout);

    if (thd->query_rule)
        thd->query_rule->timeout_thd = timeout;

    mysql_mutex_unlock(&global_timeout_cache->timeout_lock);
    return false;
}

int proxy_set_the_timeout(THD* thd, backend_conn_t* conn, int64_t exe_time_1)
{
    if (!thd->query_rule || !thd->query_rule->qpo || !thd->query_rule->timeout_thd)
        return false;

    timedout_t* timeout_thd = thd->query_rule->timeout_thd;
    if (!conn)
    {
        timeout_thd->start_time = 0;
        timeout_thd->server = NULL;
        timeout_thd->timeout = 0;
        return false;
    }

    timeout_thd->start_time = exe_time_1;
    timeout_thd->server = conn->server;
    timeout_thd->timeout = thd->query_rule->qpo->timeout;
    timeout_thd->thread_id = ((MYSQL*)(conn->get_mysql()))->thread_id;

    return false;
}

int proxy_release_thd_timeout(THD* thd)
{
    timedout_t* timeout = NULL;
    if (!thd->query_rule || thd->connection_type != PROXY_CONNECT_PROXY)
        return false;

    mysql_mutex_lock(&global_timeout_cache->timeout_lock);
    timeout = thd->query_rule->timeout_thd;
    if (timeout == NULL)
    {
        mysql_mutex_unlock(&global_timeout_cache->timeout_lock);
        return false;
    }

    thd->query_rule->timeout_thd = NULL;
    timeout->timeout = 0;
    timeout->start_time = 0;
    LIST_REMOVE(link, global_timeout_cache->full_list, timeout);
    LIST_ADD_LAST(link, global_timeout_cache->free_list, timeout);
    mysql_mutex_unlock(&global_timeout_cache->timeout_lock);
    return false;
}

