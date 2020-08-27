/* Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql_parse.h"
#include <string.h>
#include "mysqld.h"
#include "sql_db.h"
#include "sql_common.h"
#include "derror.h"
#include "mysys_err.h"
#include "item_subselect.h"
#include "set_var.h"
#include "sp.h"
#include "structs.h"
#include <md5.h>

int mysql_dup_char( char* src, char* dest, char chr);
int mysql_format_subselect(THD* thd, format_cache_node_t*   format_node, str_t* print_str,
                           st_select_lex *select_lex, bool top, my_bool parameterize);
int format_item(THD* thd, format_cache_node_t*   format_node, str_t* print_str,
                Item* item, st_select_lex *select_lex, my_bool parameterize);

unsigned long long md5_16_int(char* source)
{
    unsigned char de[17];
    char dest[64];
    memset(de, 0, 17);
    MD5_CTX md5_ex;
    MD5_Init(&md5_ex);
    MD5_Update(&md5_ex, (unsigned char*)source, strlen((char *)source));
    MD5_Final(de, &md5_ex);
    sprintf(dest,"0x");
    for(int i=0; i<8; i++)
        sprintf(dest,"%s%02x", dest, de[i]);
    unsigned long long num=strtoull(dest,NULL,0);
    return num;
}

void md5_16(char* dest, char* source)
{
    unsigned char de[17];
    memset(de, 0, 17);
    MD5_CTX md5_ex;
    MD5_Init(&md5_ex);
    MD5_Update(&md5_ex, (unsigned char*)source, strlen((char *)source));
    MD5_Final(de, &md5_ex);
    for(int i=4; i<12; i++)
        sprintf(dest,"%s%02x", dest, de[i]);
}

int
mysql_dup_char(
               char* src,
               char* dest,
               char chr
               )
{
    int ret = 0;
    char* p = src;
    while (*src)
    {
        if (*src == '\\')
            ret=1;
        //对于存在转义的情况，则不做替换
        if (*src == chr && (p == src || (*(src-1) != '\\' || chr == '\\')))
        {
            *dest=chr;
            *(++dest) = chr;
        }
        else
        {
            *dest = *src;
        }
        
        dest++;
        src++;
    }
    
    dest[0] = '\0';
    return ret;
}

int
mysql_dup_char_with_len(
               char* src,
               char* dest,
               char chr,
               int src_len
               )
{
    int ret = 0;
    while (src_len > 0)
    {
        if (*src == '\\')
            ret=1;
        if (*src == chr)
        {
            *dest=chr;
            *(++dest) = chr;
        }
        else
        {
            *dest = *src;
        }
        
        dest++;
        src++;
        src_len--;
    }
    
    dest[0] = '\0';
    return ret;
}

table_rt_t*
mysql_find_field_from_all_tables(
                                 THD* thd,
                                 rt_lst_t* rt_lst,
                                 st_select_lex *select_lex_in,
                                 const char* dbname,
                                 const char* tablename,
                                 const char* field_name
                                 )
{
    table_info_t* tableinfo = NULL;
    field_info_t* fieldinfo = NULL;
    st_select_lex *select_lex;
    check_rt_t*   rt;
    table_rt_t*         tablert;
    table_rt_t*         ret_tablert = NULL;
    
    select_lex = select_lex_in;
retry:
    rt = LIST_GET_FIRST(*rt_lst);
    while (rt != NULL)
    {
        if ((st_select_lex*)rt->select_lex == select_lex)
        {
            tablert = LIST_GET_FIRST(rt->table_rt_lst);
            while(tablert)
            {
                ret_tablert = NULL;
                tableinfo = tablert->table_info;
                fieldinfo = LIST_GET_FIRST(tableinfo->field_lst);
                while (fieldinfo != NULL) {
                    if (strcasecmp(fieldinfo->field_name, field_name) == 0) {
                        ret_tablert = tablert;
                        break;
                    }
                    
                    fieldinfo = LIST_GET_NEXT(link, fieldinfo);
                }
                
                if (ret_tablert)
                {
                    //指定库表，那就没有别名的情况了
                    if (tablename && dbname)
                    {
                        tableinfo = ret_tablert->table_info;
                        if (!strcasecmp(tableinfo->table_name, tablename) &&
                            !strcasecmp(tableinfo->db_name, dbname))
                        {
                            return ret_tablert;
                        }
                    }
                    else if (tablename)//有可能是别名，有可能是实际表名
                    {
                        tableinfo = ret_tablert->table_info;
                        if (!strcasecmp(tableinfo->table_name, tablename) ||
                            !strcasecmp(ret_tablert->alias, tablename))
                        {
                            return ret_tablert;
                        }
                    }
                    else //没有指定任何标识ID
                    {
                        return ret_tablert;
                    }
                }
                
                tablert = LIST_GET_NEXT(link, tablert);
            }
        }
        
        rt = LIST_GET_NEXT(link, rt);
    }
    
    //not found
    if (select_lex->context.outer_context)
    {
        select_lex = select_lex->context.outer_context->select_lex;
        goto retry;
    }
    else
    {
        char name[1024];
        name[0] = '\0';
        if (dbname)
        {
            strcat(name, dbname);
            strcat(name, ".");
        }
        if (tablename)
        {
            strcat(name, tablename);
            strcat(name, ".");
        }
        if (field_name)
        {
            strcat(name, field_name);
        }
        return NULL;
    }
}

int
mysql_convert_derived_table(
                            THD* thd,
                            TABLE_LIST* table,
                            check_rt_t* rt,
                            st_select_lex *select_lex
                            )
{
    table_info_t*  table_info;
    field_info_t*  field_info;
    st_select_lex_unit *derived;
    Item* item;
    table_rt_t* tablert;
    
    derived = table->derived;
    
    SELECT_LEX *last_select= derived->first_select();
    while (last_select)
    {
        table_info = (table_info_t*)malloc(sizeof(table_info_t));
        memset(table_info, 0, sizeof(table_info_t));
        LIST_INIT(table_info->field_lst);
        
        List_iterator<Item> it(last_select->item_list);
        while ((item= it++))
        {
            field_info = (field_info_t*)malloc(sizeof(field_info_t));
            memset(field_info, 0, sizeof(field_info_t));
            if (item->name)
                strcpy(field_info->field_name, item->name);
            else
                strcpy(field_info->field_name, (char*)item->full_name());
            
            LIST_ADD_LAST(link, table_info->field_lst, field_info);
        }
        
        strcpy(table_info->table_name, table->alias);
        strcpy(table_info->db_name, table->db);
        tablert = (table_rt_t*)my_malloc(sizeof(table_rt_t), MY_ZEROFILL);
        tablert->table_info = table_info;
        tablert->derived = true;
        if (table->alias)
            strcpy(tablert->alias, table->alias);
        LIST_ADD_LAST(link, rt->table_rt_lst, tablert);
        
        last_select= last_select->next_select();
    }
    
    return false;
}

int
mysql_load_tables(
                  THD* thd,
                  rt_lst_t* rt_lst,
                  st_select_lex *select_lex
                  )
{
    SQL_I_List<TABLE_LIST> * tables;
    TABLE_LIST* table;
    check_rt_t*     rt;
    
    rt = (check_rt_t*)my_malloc(sizeof(check_rt_t), MY_ZEROFILL);
    rt->select_lex = select_lex;
    
    tables = &select_lex->table_list;
    
    for (table= tables->first; table; table= table->next_local)
    {
        if (table->is_view_or_derived())
        {
            mysql_convert_derived_table(thd, table, rt, select_lex);
            continue;
        }
    }
    
    LIST_ADD_LAST(link, *rt_lst, rt);
    return 0;
}

int
format_sum_item(
    THD* thd,
    format_cache_node_t*   format_node,
    str_t* print_str,
    Item* item,
    st_select_lex *select_lex
)
{
    Item_sum *item_sum= (((Item_sum*) item));
    switch(item_sum->sum_func())
    {
    case Item_sum::COUNT_FUNC:
        str_append(print_str, "COUNT(");
        break;
    case Item_sum::COUNT_DISTINCT_FUNC:
        str_append(print_str, "COUNT(DISTINCT ");
        break;
    case Item_sum::SUM_FUNC:
        str_append(print_str, "SUM(");
        break;
    case Item_sum::SUM_DISTINCT_FUNC:
        str_append(print_str, "SUM(DISTINCT ");
        break;
    case Item_sum::AVG_FUNC:
        str_append(print_str, "AVG(");
        break;
    case Item_sum::AVG_DISTINCT_FUNC:
        str_append(print_str, "AVG(DISTINCT ");
        break;
    case Item_sum::MIN_FUNC:
        str_append(print_str, "MIN(");
        break;
    case Item_sum::MAX_FUNC:
        str_append(print_str, "MAX(");
        break;
    case Item_sum::STD_FUNC:
        str_append(print_str, "STD(");
        break;
    case Item_sum::VARIANCE_FUNC:
        str_append(print_str, "VARIANCE(");
        break;
    case Item_sum::SUM_BIT_FUNC:
        str_append(print_str, "SUMBIT(");
        break;
    case Item_sum::UDF_SUM_FUNC:
        str_append(print_str, "UDFSUM(");
        break;
    case Item_sum::GROUP_CONCAT_FUNC:
        str_append(print_str, "GROUP_CONCAT(");
        break;
    default:
        break;
    }
    
    Item* item_count = ((Item_sum_count*) item)->get_arg(0);
    format_item(thd, format_node, print_str, item_count, select_lex, proxy_format_parameterize);
    str_append(print_str, ")");
    
    return 0;
}

int
format_func_item(
    THD* thd,
    format_cache_node_t*   format_node,
    str_t* print_str,
    Item* item,
    st_select_lex *select_lex
)
{
    if (!item)
        return 0;
    switch(((Item_func *)item)->functype())
    {
    case Item_func::EQ_FUNC:
    case Item_func::NE_FUNC:
    case Item_func::LT_FUNC:
    case Item_func::LE_FUNC:
    case Item_func::GE_FUNC:
    case Item_func::GT_FUNC:
        {
            Item *left_item= ((Item_func*) item)->arguments()[0];
            format_item(thd, format_node, print_str, left_item, select_lex, proxy_format_parameterize);
            
            if (((Item_func *)item)->functype() == Item_func::EQ_FUNC)
                str_append(print_str, "=");
            else if (((Item_func *)item)->functype() == Item_func::NE_FUNC)
                str_append(print_str, "!=");
            else if (((Item_func *)item)->functype() == Item_func::LT_FUNC)
                str_append(print_str, ">");
            else if (((Item_func *)item)->functype() == Item_func::LE_FUNC)
                str_append(print_str, "<=");
            else if (((Item_func *)item)->functype() == Item_func::GE_FUNC)
                str_append(print_str, ">=");
            else if (((Item_func *)item)->functype() == Item_func::GT_FUNC)
                str_append(print_str, ">");
            
            Item *right_item= ((Item_func*) item)->arguments()[1];
            format_item(thd, format_node, print_str, right_item, select_lex, proxy_format_parameterize);
        }
        break;
    case Item_func::COND_OR_FUNC:
    case Item_func::COND_AND_FUNC:
        {
            List<Item> *args= ((Item_cond*) item)->argument_list();
            List_iterator<Item> li(*args);
            Item *item_arg;
            int first=0;
            while ((item_arg= li++))
            {
                if (first!=0)
                {
                    if (((Item_func *)item)->functype() == Item_func::COND_AND_FUNC)
                        str_append(print_str, " AND ");
                    else if (((Item_func *)item)->functype() == Item_func::COND_OR_FUNC)
                        str_append(print_str, " OR ");
                }
                format_item(thd, format_node, print_str, item_arg, select_lex, proxy_format_parameterize);
                first=1;
            }
        }
        break;
    case Item_func::ISNULL_FUNC:
    case Item_func::ISNOTNULL_FUNC:
        {
            Item *left_item= ((Item_func*) item)->arguments()[0];
            
            format_item(thd, format_node, print_str, left_item, select_lex, proxy_format_parameterize);
            
            if (((Item_func *)item)->functype() == Item_func::ISNULL_FUNC)
                str_append(print_str, " IS NULL ");
            else
                str_append(print_str, " IS NOT NULL ");
        }
        break;
    case Item_func::LIKE_FUNC:
        {
            Item *left_item= ((Item_func*) item)->arguments()[0];
            Item *right_item= ((Item_func*) item)->arguments()[1];
            
            format_item(thd, format_node, print_str, left_item, select_lex, proxy_format_parameterize);
            
            Item_func_like* like_item= dynamic_cast<Item_func_like*>(item);
            if (like_item && like_item->negated)
                str_append(print_str, " NOT");
            
            str_append(print_str, " LIKE ");
            format_item(thd, format_node, print_str, right_item, select_lex, proxy_format_parameterize);
        }
        break;
    case Item_func::BETWEEN:
        {
            Item *left_item= ((Item_func*) item)->arguments()[0];
            Item *right_item1= ((Item_func*) item)->arguments()[1];
            Item *right_item2= ((Item_func*) item)->arguments()[2];
            format_item(thd, format_node, print_str, left_item, select_lex, proxy_format_parameterize);
            
            Item_func_between* between_item= dynamic_cast<Item_func_between*>(item);
            if (between_item && between_item->negated)
                str_append(print_str, " NOT");
            
            str_append(print_str, " BETWEEN ");
            format_item(thd, format_node, print_str, right_item1, select_lex, proxy_format_parameterize);
            str_append(print_str, " AND ");
            format_item(thd, format_node, print_str, right_item2, select_lex, proxy_format_parameterize);
        }
        break;
    case Item_func::IN_FUNC:
        {
            Item *left_item= ((Item_func*) item)->arguments()[0];
            format_item(thd, format_node, print_str, left_item, select_lex, proxy_format_parameterize);
            
            Item_func_opt_neg* in_item;
            in_item = dynamic_cast<Item_func_opt_neg*>(item);
            if (in_item && in_item->negated)
                str_append(print_str, " NOT IN (");
            else
                str_append(print_str, " IN (");
            
            int need_truncate = 0;
            for (uint i=1; i < ((Item_func*) item)->argument_count();i++)
            {
                Item *right_item= ((Item_func*) item)->arguments()[i];
                format_item(thd, format_node, print_str, right_item, select_lex, proxy_format_parameterize);
                str_append(print_str, ",");
                need_truncate = 1;
            }
            if (need_truncate)
                str_truncate(print_str, 1);
            str_append(print_str, ")");
        }
        break;
    case Item_func::MULT_EQUAL_FUNC:
        {
            int need_truncate = 0;
            for (uint i=0; i < ((Item_func*) item)->argument_count();i++)
            {
                Item *right_item= ((Item_func*) item)->arguments()[i];
                format_item(thd, format_node, print_str, right_item, select_lex, proxy_format_parameterize);
                str_append(print_str, "=");
                need_truncate = 1;
            }
            if (need_truncate)
                str_truncate(print_str, 1);
        }
        break;
    case Item_func::NEG_FUNC:
        {
            Item *left_item= ((Item_func*) item)->arguments()[0];
            str_append(print_str, "-");
            format_item(thd, format_node, print_str, left_item, select_lex, proxy_format_parameterize);
        }
        break;
    case Item_func::NOT_FUNC:
        {
            Item *left_item= ((Item_func*) item)->arguments()[0];
            if ((left_item->type() != Item::SUBSELECT_ITEM && 
                dynamic_cast<Item_in_subselect*>(left_item)) &&
               (((Item_func*) item))->functype() != Item_func::LIKE_FUNC)
                str_append(print_str, " NOT ");
            format_item(thd, format_node, print_str, left_item, select_lex, proxy_format_parameterize);
        }
        break;
    case Item_func::NOW_FUNC:
        {
            str_append(print_str, "NOW()");
        }
        break;
    case Item_func::EXTRACT_FUNC:
        {
            str_append(print_str, " EXTRACT(");
            Item *left_item= ((Item_func*) item)->arguments()[0];
            format_item(thd, format_node, print_str, left_item, select_lex, proxy_format_parameterize);
            str_append(print_str, ")");
        }
        break;
    case Item_func::CASE_WHEN:
        {
            str_append(print_str, "(CASE ");
            int ncases = ((Item_func_case*) item)->get_ncases();
            int first_expr_num = ((Item_func_case*) item)->get_first_expr_num();
            int else_expr_num = ((Item_func_case*) item)->get_else_expr_num();
            if (first_expr_num > 0)
            {
                Item *right_item= ((Item_func*) item)->arguments()[first_expr_num];
                format_item(thd, format_node, print_str, right_item, select_lex, proxy_format_parameterize);
            }
            for (int i=0; i < ncases;)
            {
                Item *right_item= ((Item_func*) item)->arguments()[i++];
                str_append(print_str, " WHEN ");
                format_item(thd, format_node, print_str, right_item, select_lex, proxy_format_parameterize);
                right_item= ((Item_func*) item)->arguments()[i++];
                str_append(print_str, " THEN ");
                format_item(thd, format_node, print_str, right_item, select_lex, proxy_format_parameterize);
            }
            if (else_expr_num > 0)
            {
                str_append(print_str, " ELSE ");
                Item *right_item= ((Item_func*) item)->arguments()[else_expr_num];
                format_item(thd, format_node, print_str, right_item, select_lex, proxy_format_parameterize);
            }
            str_append(print_str, " END)");
        }
        break;
    case Item_func::FUNC_SP:
    case Item_func::UNKNOWN_FUNC:
        {
            Item_num_op* num_op = dynamic_cast<Item_num_op*>(item);
            if (!num_op)
            {
                str_append(print_str, ((Item_func*) item)->func_name());
                str_append(print_str, "(");
                int need_truncate = 0;
                for (uint i= 0; i < ((Item_func*) item)->arg_count; ++i)
                {
                    Item *right_item= ((Item_func*) item)->arguments()[i];
                    format_item(thd, format_node, print_str, right_item, select_lex, proxy_format_parameterize);
                    str_append(print_str, ",");
                    need_truncate = 1;
                }
                if (need_truncate)
                    str_truncate(print_str, 1);
                str_append(print_str, ")");
            }
            else
            {
                str_append(print_str, "(");
                int need_truncate = 0;
                for (uint i= 0; i < ((Item_func*) item)->arg_count; ++i)
                {
                    Item *right_item= ((Item_func*) item)->arguments()[i];
                    format_item(thd, format_node, print_str, right_item, select_lex, proxy_format_parameterize);
                    str_append(print_str, ((Item_func*) item)->func_name());
                    need_truncate = 1;
                }
                if (need_truncate)
                    str_truncate(print_str, 1);
                str_append(print_str, ")");
            }

        }
        break;
    case Item_func::GUSERVAR_FUNC:
    case Item_func::SUSERVAR_FUNC:
        {
            str_append(print_str, ((Item_func_user_var*)item)->name.str);
        }
        break;
    case Item_func::GSYSVAR_FUNC:
        {
            if (((Item_func_get_system_var*)item)->var_type == SHOW_OPT_GLOBAL)
                str_append(print_str, " @@global.");
            else if (((Item_func_get_system_var*)item)->var_type == SHOW_OPT_SESSION)
                str_append(print_str, " @@session.");
            else
                str_append(print_str, " @@");
            if (((Item_func_get_system_var*)item)->component.length > 0)
            {
                str_append(print_str, ((Item_func_get_system_var*)item)->component.str);
                str_append(print_str, ".");
            }
            str_append(print_str, ((Item_func_get_system_var*)item)->var->name.str);
        }
        break;
    default:
        break;
    }
    
    return 0;
}

int mysql_format_select_condition(
    THD* thd,
    format_cache_node_t*   format_node,
    str_t* print_str,
    st_select_lex *select_lex
)
{
    ORDER*   order;
    if (select_lex->where)
    {
        str_append(print_str, " WHERE ");
        format_item(thd, format_node, print_str, select_lex->where, select_lex, proxy_format_parameterize);
    }
    
    if (select_lex->group_list.elements != 0)
    {
        str_append(print_str, " GROUP BY ");
        select_lex->order_group_having = true;
        int need_truncate = 0;
        for (order= select_lex->group_list.first; order; order= order->next)
        {
            format_item(thd, format_node, print_str, *order->item, select_lex, proxy_format_parameterize);
            str_append(print_str, ",");
            need_truncate = 1;
        }
        select_lex->order_group_having = false;
        if (need_truncate)
            str_truncate(print_str, 1);
    }
    
    if (select_lex->having)
    {
        str_append(print_str, " HAVING ");
        select_lex->order_group_having = true;
        format_item(thd, format_node, print_str, select_lex->having, select_lex, proxy_format_parameterize);
        select_lex->order_group_having = false;
    }
    
    if (select_lex->order_list.elements != 0)
    {
        str_append(print_str, " ORDER BY ");
        select_lex->order_group_having = true;
        int need_truncate = 0;
        for (order= select_lex->order_list.first ; order; order= order->next)
        {
            format_item(thd, format_node, print_str, *order->item, select_lex, proxy_format_parameterize);
            str_append(print_str, ",");
            need_truncate = 1;
        }
        select_lex->order_group_having = false;
        if (need_truncate)
            str_truncate(print_str, 1);
    }
    
    if (select_lex->select_limit)
    {
        str_append(print_str, " LIMIT ");
        
        if (select_lex->offset_limit)
        {
            format_item(thd, format_node, print_str, select_lex->offset_limit, select_lex, proxy_format_parameterize);
            str_append(print_str, ",");
        }
        format_item(thd, format_node, print_str, select_lex->select_limit, select_lex, proxy_format_parameterize);
    }
    
    return 0;
}

int mysql_format_tables(
    THD* thd,
    format_cache_node_t* format_node,
    st_select_lex *select_lex,
    str_t* print_str,
    TABLE_LIST* tables
)
{
    char tablename[FN_LEN];
    char using_cond[FN_LEN];
    char table_alias[FN_LEN];
    TABLE_LIST* table;
    Item    *join_cond;
    
    if (tables)
    {
        if (thd->lex->sql_command != SQLCOM_UPDATE
            && thd->lex->sql_command != SQLCOM_UPDATE_MULTI
            && thd->lex->sql_command != SQLCOM_DELETE_MULTI)
            str_append(print_str, " FROM ");
        int first=0;
        for (table= tables; table; table= table->next_local)
        {
            if (table->outer_join == JOIN_TYPE_RIGHT)
                str_append(print_str, " RIGHT JOIN ");
            else if (table->outer_join == JOIN_TYPE_LEFT)
                str_append(print_str, " LEFT JOIN ");
            else if (table->straight == true)
                str_append(print_str, " STRAIGHT_JOIN ");
            else if (table->natural_join_type == 1)
                str_append(print_str, " JOIN ");
            else if (first==1)
                str_append(print_str, ",");
            
            first=1;
            
            if (table->is_view_or_derived())
            {
                mysql_format_subselect(thd, format_node, print_str,
                    table->derived->first_select(), false, proxy_format_parameterize);
                sprintf(tablename, " AS %s", table->alias);
                str_append(print_str, tablename);
            }
            else
            {
                sprintf(tablename, "%s.", table->db);
                str_append(print_str, tablename);
                strcpy(format_node->dbname, table->db);
                
                sprintf(tablename, "%s", table->table_name);
                str_append(print_str, tablename);
                
                if (table->is_alias)
                {
                    sprintf(table_alias, " AS %s", table->alias);
                    str_append(print_str, table_alias);
                }
                
            }
            
            if (table->join_using_fields && table->join_using_fields->elements > 0)
            {
                str_append(print_str, " USING(");
                int need_truncate = 0;
                while (table->join_using_fields->elements > 0)
                {
                    String* field= table->join_using_fields->pop();
                    sprintf(using_cond, "%s", field->ptr());
                    str_append(print_str, using_cond);
                    str_append(print_str, ",");
                    need_truncate = 1;
                }
                if (need_truncate)
                    str_truncate(print_str, 1);
                str_append(print_str, ")");
            }
            
            if (table->on_expr)
            {
                str_append(print_str, " ON ");
                
                join_cond = table->on_expr;
                if (join_cond)
                {
                    format_item(thd, format_node, print_str, join_cond, select_lex, proxy_format_parameterize);
                }
            }
        }
    }
    
    return 0;
}

int mysql_format_select(THD* thd, format_cache_node_t* format_node)
{
    char*                   dupcharsql;
    SELECT_LEX* select_lex = &thd->lex->select_lex;

    str_append_with_length(format_node->sql_statements, thd->query(), thd->query_length());
    
    if (mysql_format_subselect(thd, format_node, format_node->format_sql, select_lex, true, proxy_format_parameterize))
        return true;
    
    dupcharsql = (char*)my_malloc(str_get_len(format_node->format_sql) * 2 + 1, MYF(0));
    memset(dupcharsql, 0, str_get_len(format_node->format_sql) * 2 + 1);
    mysql_dup_char_with_len(str_get(format_node->format_sql), dupcharsql, '\\',
                            str_get_len(format_node->format_sql));
    str_truncate_0(format_node->format_sql);
    str_append_with_length(format_node->format_sql, dupcharsql, strlen(dupcharsql));

    my_free(dupcharsql);
    return false;
}


int mysql_format_subselect(
    THD* thd,
    format_cache_node_t*   format_node,
    str_t* print_str,
    st_select_lex *select_lex,
    bool top,
    my_bool parameterize
)
{
    Item* item;
    TABLE_LIST* tables;
    
    mysql_load_tables(thd, &format_node->rt_lst, select_lex);
    
    if (!top)
    {
        str_append(print_str, "(");
    }
    str_append(print_str, "SELECT ");
    List_iterator<Item> it(select_lex->item_list);
    int need_truncate = 0;
    while ((item= it++))
    {
        format_item(thd, format_node, print_str, item, select_lex, parameterize);
        if (!item->is_autogenerated_name and item->type() != Item::SUBSELECT_ITEM)
        {
            str_append(print_str, " AS ");
            str_append(print_str, item->name);
        }
        str_append(print_str, ",");
        need_truncate = 1;
    }
    if (need_truncate)
        str_truncate(print_str, 1);
    
    if (top && (thd->lex->sql_command == SQLCOM_INSERT_SELECT ||
                thd->lex->sql_command == SQLCOM_DELETE ||
                thd->lex->sql_command == SQLCOM_DELETE_MULTI ||
                thd->lex->sql_command == SQLCOM_UPDATE_MULTI ||
                thd->lex->sql_command == SQLCOM_UPDATE))
        tables = select_lex->table_list.first->next_local;
    else
        tables = select_lex->table_list.first;
    
    if (tables)
    {
        mysql_format_tables(thd, format_node, select_lex, print_str, tables);
    }
    
    mysql_format_select_condition(thd, format_node, print_str, select_lex);
    if (!top)
        str_append(print_str, ")");
    
    return 0;
}

int
format_item(
    THD* thd,
    format_cache_node_t*   format_node,
    str_t* print_str,
    Item* item,
    st_select_lex *select_lex,
    my_bool parameterize
)
{
    if (!item)
        return 0;
    switch (item->type())
    {
    case Item::STRING_ITEM:
        {
            if (parameterize)
                str_append(print_str, "?");
            else
            {
                String stringval;
                item->print(&stringval, QT_ORDINARY);
                str_append_with_length(print_str, stringval.ptr(), stringval.length());
            }
        }
        break;
    case Item::REF_ITEM:
    case Item::FIELD_ITEM:
        {
            table_info_t* tableinfo;
            table_rt_t* tablert;
            char fieldname[FN_LEN];
            char dbname[FN_LEN];
            char tablename[FN_LEN];
            dbname[0]=fieldname[0]=tablename[0] = '\0';
            
            if (!strcasecmp(((Item_field*)item)->field_name, "*"))
            {
                if (((Item_field*)item)->table_name)
                    sprintf(fieldname, "%s.", ((Item_field*)item)->field_name);
                
                sprintf(fieldname, "%s", ((Item_field*)item)->field_name);
                str_append(print_str, fieldname);
                break;
            }
            
            if (((Item_field*)item)->db_name)
            {
                sprintf(dbname, "%s.", ((Item_field*)item)->db_name);
                strcpy(format_node->dbname, ((Item_field*)item)->db_name);
            }
            if (((Item_field*)item)->table_name)
                sprintf(tablename, "%s.", ((Item_field*)item)->table_name);
            
            if (!format_node || !select_lex)
                tablert = NULL;
            else
                tablert = mysql_find_field_from_all_tables(thd, &format_node->rt_lst, 
                          select_lex, ((Item_field*)item)->db_name,
                          ((Item_field*)item)->table_name, ((Item_field*)item)->field_name);

            if (strcasecmp(((Item_field*)item)->field_name, "*"))
            {
                if (tablert && proxy_format_fullpath)
                {
                    tableinfo = tablert->table_info;
                    sprintf(tablename, "%s.", tableinfo->table_name);
                        
                    if (dbname[0] == '\0' && tableinfo->db_name[0] != '\0')
                    {
                        sprintf(dbname, "%s.", tableinfo->db_name);
                        strcpy(format_node->dbname, tableinfo->db_name);
                    }
                    if (tablename[0] == '\0')
                        sprintf(tablename, "%s.", tableinfo->table_name);
                }
                    
                str_append(print_str, dbname);
                str_append(print_str, tablename);
            }
                
            sprintf(fieldname, "%s", ((Item_field*)item)->field_name);
            str_append(print_str, fieldname);
        }
        break;
    case Item::FUNC_ITEM:
        {
            format_func_item(thd, format_node, print_str, item, select_lex);
        }
        break;
    case Item::INT_ITEM:
        {
            if (parameterize == 1)
                str_append(print_str, "?");
            else
            {
                char fieldname[FN_LEN];
                sprintf(fieldname, "%lld", ((Item_int*) item)->val_int());
                str_append(print_str, fieldname);
            }
        }
        break;
    case Item::REAL_ITEM:
        {
            if (parameterize == 1)
                str_append(print_str, "?");
            else
            {
                char fieldname[FN_LEN];
                sprintf(fieldname, "%f", ((Item_int*) item)->val_real());
                str_append(print_str, fieldname);
            }
        }
        break;
    case Item::NULL_ITEM:
        {
            str_append(print_str, "NULL");
        }
        break;
    case Item::COND_ITEM:
        {
            format_func_item(thd, format_node, print_str, item, select_lex);
        }
        break;
    case Item::SUBSELECT_ITEM:
        {
            st_select_lex *select_lex_new;
            subselect_single_select_engine* real_engine;
            Item_in_subselect* insubselect;
            
            insubselect = dynamic_cast<Item_in_subselect*>(item);
            if (insubselect && insubselect->left_expr)
            {
                format_item(thd, format_node, print_str, 
                    insubselect->left_expr, select_lex, parameterize);
                if (insubselect->not_in_subselect)
                    str_append(print_str, " NOT IN ");
                else
                    str_append(print_str, " IN ");
            }
            
            subselect_engine *engine = ((Item_subselect*)item)->engine;
            subselect_single_select_engine::enum_engine_type engine_type = engine->engine_type();
            
            if (engine_type == subselect_engine::SINGLE_SELECT_ENGINE)
            {
                real_engine = (subselect_single_select_engine*)engine;
                select_lex_new = real_engine->get_st_select_lex();
                if (mysql_format_subselect(thd, format_node, 
                      print_str, select_lex_new, false, parameterize))
                    return true;
                if (!item->is_autogenerated_name)
                {
                    str_append(print_str, " AS ");
                    str_append(print_str, item->name);
                }
            }
        }
        break;
    case Item::SUM_FUNC_ITEM:
        {
            format_sum_item(thd, format_node, print_str, item, select_lex);
        }
        break;
    case Item::ROW_ITEM:
        {
            int need_truncate = 0;
            for (uint i=0; i < ((Item_row*)item)->cols();i++)
            {
                Item *right_item= ((Item_row*)item)->element_index(i);
                format_item(thd, format_node, print_str, right_item, select_lex, parameterize);
                str_append(print_str, ",");
                need_truncate = 1;
            }
            if (need_truncate)
                str_truncate(print_str, 1);
        }
        break;
    case Item::DECIMAL_ITEM:
        {
            if (parameterize == 1)
                str_append(print_str, "?");
            else
            {
                String stringval;
                //String tmp;
                //char* fieldname;
                item->print(&stringval, QT_ORDINARY);
                //stringval = ((Item_string*) item)->val_str(&tmp);
                //fieldname= (char*)my_malloc(stringval.length() + 10, MY_ZEROFILL);
                //sprintf(fieldname, "%s", stringval.ptr());
                str_append_with_length(print_str, stringval.ptr(), stringval.length());
                //my_free(fieldname);
            }
        }
        break;
    default:
        break;
    }
    
    return 0;
}

int mysql_format_not_support(THD* thd, format_cache_node_t* format_node)
{
    return true;
}

int mysql_format_insert(THD* thd, format_cache_node_t* format_node)
{
    char*                   dupcharsql;
    SELECT_LEX* select_lex = &thd->lex->select_lex;
    Item* item;
    List_item *values;
    char tablename[FN_LEN];

    str_append_with_length(format_node->sql_statements, thd->query(), thd->query_length());
    
    mysql_load_tables(thd, &format_node->rt_lst, select_lex);
    
    str_append(format_node->format_sql, "INSERT INTO ");
    
    strcpy(format_node->dbname, thd->lex->query_tables->db);
    sprintf(tablename, "%s.", thd->lex->query_tables->db);
    str_append(format_node->format_sql, tablename);
    
    sprintf(tablename, "%s", thd->lex->query_tables->table_name);
    str_append(format_node->format_sql, tablename);
    
    if (thd->lex->field_list.elements > 0)
    {
        str_append(format_node->format_sql, "(");
        List_iterator<Item> it(thd->lex->field_list);
        while ((item= it++))
        {
            format_item(thd, format_node, format_node->format_sql, item, &thd->lex->select_lex, proxy_format_parameterize);
            str_append(format_node->format_sql, ",");
        }
        
        str_truncate(format_node->format_sql, 1);
        str_append(format_node->format_sql, ")");
    }
    
    if (thd->lex->sql_command != SQLCOM_INSERT_SELECT)
    {
        List<List_item> &values_list = thd->lex->many_values;
        List_iterator_fast<List_item> its(values_list);
        str_append(format_node->format_sql, " VALUES");
        while ((values = its++))
        {
            str_append(format_node->format_sql, "(");
            List_iterator<Item> it(*values);
            while ((item= it++))
            {
                format_item(thd, format_node, format_node->format_sql, item, &thd->lex->select_lex, proxy_format_parameterize);
                str_append(format_node->format_sql, ",");
            }
            str_truncate(format_node->format_sql, 1);
            str_append(format_node->format_sql, "),");
        }
        str_truncate(format_node->format_sql, 1);
    }
    else
    {
        str_append(format_node->format_sql, " ");
        if (mysql_format_subselect(thd, format_node, format_node->format_sql, select_lex, true, proxy_format_parameterize))
            return true;
    }
    
    if (thd->lex->duplicates == DUP_UPDATE)
    {
        str_append(format_node->format_sql, " ON DUPLICATE KEY UPDATE ");
        Item* item_it;
        Item* item_vit;
        List_iterator<Item> it(thd->lex->update_list);
        List_iterator<Item> vit(thd->lex->value_list);
        
        while ((item_it= it++) && (item_vit= vit++))
        {
            format_item(thd, format_node, format_node->format_sql, item_it, &thd->lex->select_lex, proxy_format_parameterize);
            str_append(format_node->format_sql, "=");
            format_item(thd, format_node, format_node->format_sql, item_vit, &thd->lex->select_lex, proxy_format_parameterize);
            str_append(format_node->format_sql, ",");
        }
        str_truncate(format_node->format_sql, 1);
    }

    dupcharsql = (char*)my_malloc(str_get_len(format_node->format_sql) * 2 + 1, MYF(0));
    memset(dupcharsql, 0, str_get_len(format_node->format_sql) * 2 + 1);
    mysql_dup_char_with_len(str_get(format_node->format_sql), dupcharsql, '\\',
                            str_get_len(format_node->format_sql));
    str_truncate_0(format_node->format_sql);
    str_append_with_length(format_node->format_sql, dupcharsql, strlen(dupcharsql));
    
    my_free(dupcharsql);
    return false;
}

int mysql_format_delete(THD* thd, format_cache_node_t* format_node)
{
    char*                   dupcharsql;
    SELECT_LEX* select_lex = &thd->lex->select_lex;

    str_append_with_length(format_node->sql_statements, thd->query(), thd->query_length());
    
    mysql_load_tables(thd, &format_node->rt_lst, select_lex);
    
    str_append(format_node->format_sql, "DELETE");
    if (thd->lex->auxiliary_table_list.first)
    {
        str_append(format_node->format_sql, " ");
        mysql_format_tables(thd, format_node, select_lex, format_node->format_sql,
            thd->lex->auxiliary_table_list.first);
        str_append(format_node->format_sql, " FROM ");
        mysql_format_tables(thd, format_node, select_lex, format_node->format_sql,
            thd->lex->query_tables);
    }
    else
        mysql_format_tables(thd, format_node, select_lex, format_node->format_sql,
            thd->lex->query_tables);
    
    mysql_format_select_condition(thd, format_node, format_node->format_sql, select_lex);
    
    dupcharsql = (char*)my_malloc(str_get_len(format_node->format_sql) * 2 + 1, MYF(0));
    memset(dupcharsql, 0, str_get_len(format_node->format_sql) * 2 + 1);
    mysql_dup_char_with_len(str_get(format_node->format_sql), dupcharsql, '\\',
                            str_get_len(format_node->format_sql));
    str_truncate_0(format_node->format_sql);
    str_append_with_length(format_node->format_sql, dupcharsql, strlen(dupcharsql));
    
    my_free(dupcharsql);
    return false;
}

int mysql_format_update(THD* thd, format_cache_node_t* format_node)
{
    char*                         dupcharsql;
    SELECT_LEX* select_lex = &thd->lex->select_lex;
    Item* item_it;
    Item* item_vit;

    str_append_with_length(format_node->sql_statements, thd->query(), thd->query_length());
    
    mysql_load_tables(thd, &format_node->rt_lst, select_lex);

    str_append(format_node->format_sql, "UPDATE ");
    
    mysql_format_tables(thd, format_node, select_lex, format_node->format_sql,
        thd->lex->query_tables);
    
    str_append(format_node->format_sql, " SET ");
    
    List_iterator<Item> it(thd->lex->select_lex.item_list);
    List_iterator<Item> vit(thd->lex->value_list);
    
    while ((item_it= it++) && (item_vit= vit++))
    {
        format_item(thd, format_node, format_node->format_sql, item_it, &thd->lex->select_lex, proxy_format_parameterize);
        str_append(format_node->format_sql, "=");
        format_item(thd, format_node, format_node->format_sql, item_vit, &thd->lex->select_lex, proxy_format_parameterize);
        str_append(format_node->format_sql, ",");
    }
    
    str_truncate(format_node->format_sql, 1);
    
    mysql_format_select_condition(thd, format_node, format_node->format_sql, select_lex);
    
    dupcharsql = (char*)my_malloc(str_get_len(format_node->format_sql) * 2 + 1, MYF(0));
    memset(dupcharsql, 0, str_get_len(format_node->format_sql) * 2 + 1);
    mysql_dup_char_with_len(str_get(format_node->format_sql), dupcharsql, '\\',
                            str_get_len(format_node->format_sql));
    str_truncate_0(format_node->format_sql);
    str_append_with_length(format_node->format_sql, dupcharsql, strlen(dupcharsql));
    
    my_free(dupcharsql);
    return false;
}

int mysql_format_change_db(THD* thd, format_cache_node_t* format_node)
{
    str_append_with_length(format_node->sql_statements, thd->query(), thd->query_length());
    LEX_STRING db_str= { (char *) thd->lex->select_lex.db, strlen(thd->lex->select_lex.db) };
    mysql_change_db(thd, &db_str, FALSE);
    
    str_append(format_node->format_sql, "USE ");
    str_append(format_node->format_sql, db_str.str);
    strcpy(format_node->dbname, db_str.str);
    
    return false;
}

int mysql_format_set(THD* thd, format_cache_node_t* format_node)
{
    int error;
    String set_names_str;
    
    DBUG_ENTER("mysql_format_set");
    str_append_with_length(format_node->sql_statements, thd->query(), thd->query_length());
    
    List_iterator_fast<set_var_base> it(thd->lex->var_list);
    
    set_var_base *var;
    while ((var=it++))
    {
        //DBA执行的语句，需要设置的，只支持set names ...语句
        if (dynamic_cast <set_var_collation_client*> (var))
        {
            if ((error= var->check(thd)))
                DBUG_RETURN(TRUE);
            
            if ((error = var->update(thd)))        // Returns 0, -1 or 1
                DBUG_RETURN(TRUE);
            
            dynamic_cast <set_var_collation_client*> (var)->print(thd, &set_names_str);
            
            str_append(format_node->format_sql, "SET ");
            str_append(format_node->format_sql, set_names_str.c_ptr());
        }
        else
            DBUG_RETURN(TRUE);
    }
    //Set statement may contain single quotes like: SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci';.
    char* dupcharsql;
    dupcharsql = (char*)my_malloc(str_get_len(format_node->sql_statements) * 2 + 1, MYF(0));
    memset(dupcharsql, 0, str_get_len(format_node->sql_statements) * 2 + 1);
    mysql_dup_char_with_len(str_get(format_node->sql_statements), dupcharsql, '\'',
                            str_get_len(format_node->sql_statements));
    str_truncate_0(format_node->format_sql);
    str_append_with_length(format_node->format_sql, dupcharsql, strlen(dupcharsql));
    my_free(dupcharsql);

    DBUG_RETURN(FALSE);
}

void
mysql_table_info_free(
                      table_info_t* table_info
                      )
{
    field_info_t*  field_info;
    field_info_t*  next_field_info;
    
    DBUG_ENTER("mysql_table_info_free");
    
    if (table_info == NULL)
        DBUG_VOID_RETURN;
    
    field_info = LIST_GET_FIRST(table_info->field_lst);
    while (field_info != NULL)
    {
        next_field_info = LIST_GET_NEXT(link, field_info);
        LIST_REMOVE(link, table_info->field_lst, field_info);
        
        if (field_info->field)
        {
            delete field_info->field;
            field_info->field = NULL;
        }
        
        free(field_info);
        field_info = next_field_info;
    }
    
    my_free(table_info->record);
    my_free(table_info->null_arr);
    free(table_info);
    
    DBUG_VOID_RETURN;
}

int deinit_format_cache_node(format_cache_node_t* format_cache_node, int free_all)
{
    table_rt_t*     table_rt;
    table_rt_t*     table_rt_next;
    check_rt_t*     query_rt;
    check_rt_t*     query_rt_next;

    if (free_all)
    {
        str_deinit(format_cache_node->sql_statements);
        str_deinit(format_cache_node->format_sql);
        str_deinit(format_cache_node->errmsg);
        if (format_cache_node->sql_statements)
            my_free(format_cache_node->sql_statements);
        if (format_cache_node->format_sql)
            my_free(format_cache_node->format_sql);
        if (format_cache_node->errmsg)
            my_free(format_cache_node->errmsg);
    }

    query_rt = LIST_GET_FIRST(format_cache_node->rt_lst);
    while (query_rt)
    {
        query_rt_next = LIST_GET_NEXT(link, query_rt);
        LIST_REMOVE(link, format_cache_node->rt_lst, query_rt);

        table_rt = LIST_GET_FIRST(query_rt->table_rt_lst);
        while(table_rt)
        {
            table_rt_next = LIST_GET_NEXT(link, table_rt);
            LIST_REMOVE(link, query_rt->table_rt_lst, table_rt);
            if (table_rt->derived)
                mysql_table_info_free(table_rt->table_info);
            my_free(table_rt);
            table_rt = table_rt_next;
        }

        my_free(query_rt);
        query_rt = query_rt_next;
    }

    if (free_all)
        my_free(format_cache_node);
    return true;
}

int mysql_deinit_format_cache(format_cache_t* format_cache)
{
    format_cache_node_t * format_cache_node;
    format_cache_node_t * format_cache_node_next;
    table_rt_t*                 table_rt;
    table_rt_t*                 table_rt_next;
    check_rt_t*   query_rt;
    check_rt_t*   query_rt_next;
    
    DBUG_ENTER("mysql_deinit_format_cache");
    
    if (format_cache != NULL)
    {
        format_cache_node = LIST_GET_FIRST(format_cache->field_lst);
        while (format_cache_node != NULL)
        {
            format_cache_node_next = LIST_GET_NEXT(link, format_cache_node);
            str_deinit(format_cache_node->sql_statements);
            str_deinit(format_cache_node->format_sql);
            str_deinit(format_cache_node->errmsg);
            if (format_cache_node->sql_statements)
                my_free(format_cache_node->sql_statements);
            if (format_cache_node->format_sql)
                my_free(format_cache_node->format_sql);
            if (format_cache_node->errmsg)
                my_free(format_cache_node->errmsg);
            
            query_rt = LIST_GET_FIRST(format_cache_node->rt_lst);
            while (query_rt)
            {
                query_rt_next = LIST_GET_NEXT(link, query_rt);
                LIST_REMOVE(link, format_cache_node->rt_lst, query_rt);
                
                table_rt = LIST_GET_FIRST(query_rt->table_rt_lst);
                while(table_rt)
                {
                    table_rt_next = LIST_GET_NEXT(link, table_rt);
                    LIST_REMOVE(link, query_rt->table_rt_lst, table_rt);
                    if (table_rt->derived)
                        mysql_table_info_free(table_rt->table_info);
                    my_free(table_rt);
                    table_rt = table_rt_next;
                }
                
                my_free(query_rt);
                query_rt = query_rt_next;
            }
            
            my_free(format_cache_node);
            format_cache_node = format_cache_node_next;
        }
        my_free(format_cache);
        format_cache= NULL;
    }
    
    DBUG_RETURN(FALSE);
}

int
mysql_format_delete_to_select(THD* thd,str_t* str_select)
{
    char*                   dupcharsql;
    format_cache_node_t*    format_node;

    SELECT_LEX* select_lex = &thd->lex->select_lex;

    format_node = (format_cache_node_t*)my_malloc(sizeof(format_cache_node_t), MY_ZEROFILL);
    format_node->sql_statements = (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
    format_node->format_sql= (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
    str_init(format_node->format_sql);
    str_init(format_node->sql_statements);
    str_append_with_length(format_node->sql_statements, thd->query(), thd->query_length());

    str_append(format_node->format_sql, "SELECT COUNT(*)");
    if (thd->lex->sql_command == SQLCOM_DELETE_MULTI)
        str_append(format_node->format_sql, " FROM ");

    mysql_load_tables(thd, &format_node->rt_lst, select_lex);

    mysql_format_tables(thd, format_node, select_lex, format_node->format_sql,
                            thd->lex->query_tables);

    mysql_format_select_condition(thd, format_node, format_node->format_sql, select_lex);

    dupcharsql = (char*)my_malloc(str_get_len(format_node->format_sql) * 2 + 1, MYF(0));
    memset(dupcharsql, 0, str_get_len(format_node->format_sql) * 2 + 1);
    mysql_dup_char_with_len(str_get(format_node->format_sql), dupcharsql, '\\',
                            str_get_len(format_node->format_sql));
    str_append_with_length(str_select, dupcharsql, strlen(dupcharsql));

    deinit_format_cache_node(format_node, true);
    my_free(dupcharsql);
    return 0;
}

int
mysql_format_update_to_select(THD* thd,str_t* str_select)
{
    char*                         dupcharsql;
    format_cache_node_t*         format_node;
    SELECT_LEX* select_lex = &thd->lex->select_lex;
    Item* item_it;
    Item* item_vit;

    format_node = (format_cache_node_t*)my_malloc(sizeof(format_cache_node_t), MY_ZEROFILL);
    format_node->sql_statements = (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
    format_node->format_sql= (str_t*)my_malloc(sizeof(str_t), MY_ZEROFILL);
    str_init(format_node->format_sql);
    str_init(format_node->sql_statements);
    str_append_with_length(format_node->sql_statements, thd->query(), thd->query_length());

    mysql_load_tables(thd, &format_node->rt_lst, select_lex);

    str_append(format_node->format_sql, "SELECT ");

    List_iterator<Item> it(thd->lex->select_lex.item_list);
    List_iterator<Item> vit(thd->lex->value_list);

    while ((item_it= it++) && (item_vit= vit++))
    {
        format_item(thd, format_node, format_node->format_sql, item_it, &thd->lex->select_lex, proxy_format_parameterize);
        str_append(format_node->format_sql, ",");
    }

    str_truncate(format_node->format_sql, 1);

    str_append(format_node->format_sql, " FROM ");

    mysql_format_tables(thd, format_node, select_lex, format_node->format_sql,
                        thd->lex->query_tables);

    mysql_format_select_condition(thd, format_node, format_node->format_sql, select_lex);

    dupcharsql = (char*)my_malloc(str_get_len(format_node->format_sql) * 2 + 1, MYF(0));
    memset(dupcharsql, 0, str_get_len(format_node->format_sql) * 2 + 1);
    mysql_dup_char_with_len(str_get(format_node->format_sql), dupcharsql, '\\',
                            str_get_len(format_node->format_sql));
    str_append_with_length(str_select, dupcharsql, strlen(dupcharsql));

    deinit_format_cache_node(format_node, true);
    my_free(dupcharsql);
    return false;
}

int get_grant_sql(THD* thd, str_t* sql, bool hash_user)
{
    str_append(sql, "GRANT ");
    uint grant = thd->lex->grant;
    
    if (grant & SELECT_ACL)
        str_append(sql,"SELECT, ");
    if (grant & INSERT_ACL)
        str_append(sql,"INSERT, ");
    if (grant & UPDATE_ACL)
        str_append(sql,"UPDATE, ");
    if (grant & DELETE_ACL)
        str_append(sql,"DELETE, ");
    if (grant & CREATE_ACL)
        str_append(sql,"CREATE, ");
    if (grant & DROP_ACL)
        str_append(sql,"DROP, ");
    if (grant & RELOAD_ACL)
        str_append(sql,"RELOAD, ");
    if (grant & SHUTDOWN_ACL)
        str_append(sql,"SHUTDOWN, ");
    if (grant & PROCESS_ACL)
        str_append(sql,"PROCESS, ");
    if (grant & FILE_ACL)
        str_append(sql,"FILE, ");
    if (grant & REFERENCES_ACL)
        str_append(sql,"REFERENCES, ");
    if (grant & INDEX_ACL)
        str_append(sql,"INDEX, ");
    if (grant & ALTER_ACL)
        str_append(sql,"ALTER, ");
    if (grant & SHOW_DB_ACL)
        str_append(sql,"SHOW DATABASES, ");
    if (grant & SUPER_ACL)
        str_append(sql,"SUPER, ");
    if (grant & CREATE_TMP_ACL)
        str_append(sql,"CREATE TEMPORARY TABLES, ");
    if (grant & LOCK_TABLES_ACL)
        str_append(sql,"LOCK TABLES, ");
    if (grant & EXECUTE_ACL)
        str_append(sql,"EXECUTE, ");
    if (grant & REPL_SLAVE_ACL)
        str_append(sql,"REPLICATION SLAVE, ");
    if (grant & REPL_CLIENT_ACL)
        str_append(sql,"REPLICATION CLIENT, ");
    if (grant & CREATE_VIEW_ACL)
        str_append(sql,"CREATE VIEW, ");
    if (grant & SHOW_VIEW_ACL)
        str_append(sql,"SHOW VIEW, ");
    if (grant & CREATE_PROC_ACL)
        str_append(sql,"CREATE ROUTINE, ");
    if (grant & ALTER_PROC_ACL)
        str_append(sql,"ALTER ROUTINE, ");
    if (grant & CREATE_USER_ACL)
        str_append(sql,"CREATE USER, ");
    if (grant & EVENT_ACL)
        str_append(sql,"EVENT, ");
    if (grant & TRIGGER_ACL)
        str_append(sql,"TRIGGER, ");
    if (grant & CREATE_TABLESPACE_ACL)
        str_append(sql,"CREATE TABLESPACE ");
    else
        str_append(sql,"USAGE ");
    
    str_append(sql, "ON ");
    
    if (thd->lex->type == TYPE_ENUM_FUNCTION)
    {
        str_append(sql, " FUNCTION ");
    }
    else if (thd->lex->type == TYPE_ENUM_PROCEDURE)
    {
        str_append(sql, " PROCEDURE ");
    }
    
    if (thd->lex->select_lex.table_list.elements > 0)
    {
        TABLE_LIST* tl = thd->lex->select_lex.table_list.first;
        str_append(sql, tl->db);
        str_append(sql, ".");
        str_append(sql, tl->table_name);
    }else if (thd->lex->select_lex.db != NULL)
    {
        str_append(sql, thd->lex->select_lex.db);
        str_append(sql, ".*");
        
    }else
    {
        str_append(sql, "*.* ");
    }
    str_append(sql, " TO ");

    st_lex_user* user;
    
    for (uint i=0; i < thd->lex->users_list.elements; ++i)
    {
        user = thd->lex->users_list.elem(i);
        if (hash_user && check_username_need_hash(user->host.str, user->host.length))
        {
            char new_user[130] = {0};
            char new_user_hash[17] = {0};
            sprintf(new_user, "%s_%s", user->user.str, user->host.str);
            md5_16(new_user_hash, new_user);
            str_append(sql, "'");
            str_append(sql, new_user_hash);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
            str_append(sql, "'");
        }
        else
        {
            str_append(sql, "'");
            str_append(sql, user->user.str);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);

            str_append(sql, "'");
        }

        if (user->pwtext.length > 0)
        {
            str_append(sql, " IDENTIFIED BY '");
            str_append(sql, user->pwtext.str);
            str_append(sql, "'");
        }
        else if(user->pwhash.length > 0)
        {
            str_append(sql, " IDENTIFIED BY PASSWORD '");
            str_append(sql, user->pwhash.str);
            str_append(sql, "'");
        }
        else if (user->plugin.length > 0)
        {
            str_append(sql, " IDENTIFIED WITH '");
            str_append(sql, user->plugin.str);
            str_append(sql, "'");
            if (user->auth.length > 0)
            {
                if (user->using_clause == 0)
                    str_append(sql, " USING '");
                else if (user->using_clause == 1)
                    str_append(sql, " AS '");
                else if (user->using_clause == 2)
                    str_append(sql, " BY '");
                str_append(sql, user->auth.str);
                str_append(sql, "'");
            }
        }

        if (i < thd->lex->users_list.elements-1)
        {
            str_append(sql, ", ");
        }
    }
    
    if (thd->lex->ssl_type == SSL_TYPE_NONE)
    {
        str_append(sql, " REQUIRE NONE ");
    }
    else if (thd->lex->ssl_type == SSL_TYPE_ANY)
    {
        str_append(sql, " REQUIRE SSL ");
    }
    else if (thd->lex->ssl_type == SSL_TYPE_X509)
    {
        str_append(sql, " REQUIRE X509 ");
    }
    else if (thd->lex->ssl_type == SSL_TYPE_SPECIFIED)
    {
        int j = 0;
        if (thd->lex->x509_issuer)
        {
            str_append(sql, " REQUIRE CIPHER '");
            str_append(sql, thd->lex->x509_issuer);
            str_append(sql, "'");
            j = 1;
        }
        
        if (j == 1 && thd->lex->x509_subject)
        {
            str_append(sql, " AND SUBJECT '");
            str_append(sql, thd->lex->x509_subject);
            str_append(sql, "'");
        }
        else if (thd->lex->x509_subject)
        {
            str_append(sql, " REQUIRE SUBJECT '");
            str_append(sql, thd->lex->x509_subject);
            str_append(sql, "'");
            j = 1;
        }
        
        if (j == 1 && thd->lex->ssl_cipher)
        {
            str_append(sql, " AND CIPHER '");
            str_append(sql, thd->lex->ssl_cipher);
            str_append(sql, "'");
        }
        else if (thd->lex->ssl_cipher)
        {
            str_append(sql, " REQUIRE CIPHER '");
            str_append(sql, thd->lex->ssl_cipher);
            str_append(sql, "'");
            j = 1;
        }
    }
    
    if (grant & GRANT_ACL)
    {
        str_append(sql, " WITH GRANT OPTION");
    }
    else if (thd->lex->mqh.specified_limits > 0)
    {
        str_append(sql, " WITH ");
        char tmp[50];
        if (thd->lex->mqh.specified_limits & user_resources::QUERIES_PER_HOUR)
        {
            sprintf(tmp, "%d", thd->lex->mqh.questions);
            str_append(sql, " MAX_QUERIES_PER_HOUR ");
            str_append(sql, tmp);
        }
        if (thd->lex->mqh.specified_limits & user_resources::UPDATES_PER_HOUR)
        {
            sprintf(tmp, "%d", thd->lex->mqh.updates);
            str_append(sql, " MAX_UPDATES_PER_HOUR ");
            str_append(sql, tmp);
        }
        if (thd->lex->mqh.specified_limits & user_resources::CONNECTIONS_PER_HOUR)
        {
            sprintf(tmp, "%d", thd->lex->mqh.conn_per_hour);
            str_append(sql, " MAX_CONNECTIONS_PER_HOUR ");
            str_append(sql, tmp);
        }
        if (thd->lex->mqh.specified_limits & user_resources::USER_CONNECTIONS)
        {
            sprintf(tmp, "%d", thd->lex->mqh.user_conn);
            str_append(sql, " MAX_USER_CONNECTIONS ");
            str_append(sql, tmp);
        }
    }

    return false;
}

int mysql_format_grant(THD* thd, format_cache_node_t* format_node)
{
    char* dupcharsql;
    str_append_with_length(format_node->sql_statements, thd->query(), thd->query_length());
    get_grant_sql(thd, format_node->format_sql, false);
    dupcharsql = (char*)my_malloc(str_get_len(format_node->format_sql) * 2 + 1, MYF(0));
    memset(dupcharsql, 0, str_get_len(format_node->format_sql) * 2 + 1);
    mysql_dup_char_with_len(str_get(format_node->format_sql), dupcharsql, '\'',
                            str_get_len(format_node->format_sql));
    str_truncate_0(format_node->format_sql);
    str_append_with_length(format_node->format_sql, dupcharsql, strlen(dupcharsql));

    my_free(dupcharsql);
    return false;
}

int get_revoke_sql(THD* thd, str_t* sql, bool hash_user)
{
    str_append(sql, "REVOKE ");
    uint grant = thd->lex->grant;
    
    if (grant & SELECT_ACL)
        str_append(sql,"SELECT, ");
    if (grant & INSERT_ACL)
        str_append(sql,"INSERT, ");
    if (grant & UPDATE_ACL)
        str_append(sql,"UPDATE, ");
    if (grant & DELETE_ACL)
        str_append(sql,"DELETE, ");
    if (grant & CREATE_ACL)
        str_append(sql,"CREATE, ");
    if (grant & DROP_ACL)
        str_append(sql,"DROP, ");
    if (grant & RELOAD_ACL)
        str_append(sql,"RELOAD, ");
    if (grant & SHUTDOWN_ACL)
        str_append(sql,"SHUTDOWN, ");
    if (grant & PROCESS_ACL)
        str_append(sql,"PROCESS, ");
    if (grant & FILE_ACL)
        str_append(sql,"FILE, ");
    if (grant & REFERENCES_ACL)
        str_append(sql,"REFERENCES, ");
    if (grant & INDEX_ACL)
        str_append(sql,"INDEX, ");
    if (grant & ALTER_ACL)
        str_append(sql,"ALTER, ");
    if (grant & SHOW_DB_ACL)
        str_append(sql,"SHOW DATABASES, ");
    if (grant & SUPER_ACL)
        str_append(sql,"SUPER, ");
    if (grant & CREATE_TMP_ACL)
        str_append(sql,"CREATE TEMPORARY TABLES, ");
    if (grant & LOCK_TABLES_ACL)
        str_append(sql,"LOCK TABLES, ");
    if (grant & EXECUTE_ACL)
        str_append(sql,"EXECUTE, ");
    if (grant & REPL_SLAVE_ACL)
        str_append(sql,"REPLICATION SLAVE, ");
    if (grant & REPL_CLIENT_ACL)
        str_append(sql,"REPLICATION CLIENT, ");
    if (grant & CREATE_VIEW_ACL)
        str_append(sql,"CREATE VIEW, ");
    if (grant & SHOW_VIEW_ACL)
        str_append(sql,"SHOW VIEW, ");
    if (grant & CREATE_PROC_ACL)
        str_append(sql,"CREATE ROUTINE, ");
    if (grant & ALTER_PROC_ACL)
        str_append(sql,"ALTER ROUTINE, ");
    if (grant & CREATE_USER_ACL)
        str_append(sql,"CREATE USER, ");
    if (grant & EVENT_ACL)
        str_append(sql,"EVENT, ");
    if (grant & TRIGGER_ACL)
        str_append(sql,"TRIGGER, ");
    if (grant & CREATE_TABLESPACE_ACL)
        str_append(sql,"CREATE TABLESPACE ");
    else
        str_append(sql,"USAGE ");
    
    str_append(sql, "ON ");
    
    if (thd->lex->type == TYPE_ENUM_FUNCTION)
    {
        str_append(sql, " FUNCTION ");
    }
    else if (thd->lex->type == TYPE_ENUM_PROCEDURE)
    {
        str_append(sql, " PROCEDURE ");
    }
    
    if (thd->lex->select_lex.table_list.elements > 0)
    {
        TABLE_LIST* tl = thd->lex->select_lex.table_list.first;
        str_append(sql, tl->db);
        str_append(sql, ".");
        str_append(sql, tl->table_name);
        
    }else if (thd->lex->select_lex.db != NULL)
    {
        str_append(sql, thd->lex->select_lex.db);
        str_append(sql, ".*");
        
    }else
    {
        str_append(sql, "*.* ");
    }
    
    str_append(sql, " FROM ");
    st_lex_user* user;

    for (uint i=0; i < thd->lex->users_list.elements; ++i)
    {
        user = thd->lex->users_list.elem(i);
        if (hash_user && check_username_need_hash(user->host.str, user->host.length))
        {
            char new_user[130] = {0};
            char new_user_hash[17] = {0};
            sprintf(new_user, "%s_%s", user->user.str, user->host.str);
            md5_16(new_user_hash, new_user);
            str_append(sql, " '");
            str_append(sql, new_user_hash);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
            str_append(sql, "'");
        }
        else
        {
            str_append(sql, " '");
            str_append(sql, user->user.str);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
            str_append(sql, "'");
        }
        
        if (i < thd->lex->users_list.elements-1)
        {
            str_append(sql, ", ");
        }
    }

    return false;
}

int mysql_format_others(THD* thd, format_cache_node_t* format_node)
{
    char* dupcharsql;

    str_append_with_length(format_node->sql_statements, thd->query(), thd->query_length());
    dupcharsql = (char*)my_malloc(str_get_len(format_node->sql_statements) * 2 + 1, MYF(0));
    memset(dupcharsql, 0, str_get_len(format_node->sql_statements) * 2 + 1);
    mysql_dup_char_with_len(str_get(format_node->sql_statements), dupcharsql, '\'',
                            str_get_len(format_node->sql_statements));
    str_truncate_0(format_node->format_sql);
    str_append_with_length(format_node->format_sql, dupcharsql, strlen(dupcharsql));

    my_free(dupcharsql);
    return false;
}

int mysql_format_revoke(THD* thd, format_cache_node_t* format_node)
{
    char* dupcharsql;

    str_append_with_length(format_node->sql_statements, thd->query(), thd->query_length());
    get_revoke_sql(thd, format_node->format_sql, false);
    dupcharsql = (char*)my_malloc(str_get_len(format_node->format_sql) * 2 + 1, MYF(0));
    memset(dupcharsql, 0, str_get_len(format_node->format_sql) * 2 + 1);
    mysql_dup_char_with_len(str_get(format_node->format_sql), dupcharsql, '\'',
                            str_get_len(format_node->format_sql));
    str_truncate_0(format_node->format_sql);
    str_append_with_length(format_node->format_sql, dupcharsql, strlen(dupcharsql));

    my_free(dupcharsql);
    return false;
}

int get_kill_sql(THD* thd, str_t* sql, bool hash_user, char* thread_id)
{
    LEX *lex= thd->lex;
    str_append(sql, "KILL ");
   
//    MySQL不支持这玩意，MariaDB记几的语法。
//    if (lex->kill_signal == 0)
//    {
//        str_append(sql, "SOFT ");
//    }
//    else
//    {
//        str_append(sql, "HARD ");
//    }

    if (lex->type == 0)
    {
        str_append(sql, "CONNECTION ");
    }
    else if (lex->type == 1)
    {
        str_append(sql, "QUERY ");
    }

    if (lex->kill_type == KILL_TYPE_ID || lex->kill_type == KILL_TYPE_QUERY)
    {
        if (thread_id)
        {
            str_append(sql, thread_id);
        }
        else
        {
            char tmp_value[65] = {0};
            sprintf(tmp_value, "%lld", lex->value_list.head()->val_int());
            str_append(sql, tmp_value);
        }
    }
//    else
//    {
//        str_append(sql, "USER ");
//        st_lex_user* user = thd->lex->users_list.head();
//        if (hash_user && !check_white_ip(user->host.str, user->host.length))
//        {
//            char new_user[130] = {0};
//            char new_user_hash[17] = {0};
//            sprintf(new_user, "%s_%s", user->user.str, user->host.str);
//            md5_16(new_user_hash, new_user);
//            str_append(sql, new_user_hash);
//        }
//        else
//        {
//            str_append(sql, user->user.str);
//        }
//    }
    return false;
}

int get_drop_user_sql(THD* thd, str_t* sql, bool hash_user)
{
    str_append(sql, "DROP USER ");
    if (thd->lex->if_exists())
    {
         str_append(sql, "IF EXISTS ");
    }

    st_lex_user* user;
    for (uint i=0; i < thd->lex->users_list.elements; ++i)
    {
        user = thd->lex->users_list.elem(i);
        str_append(sql, "'");
        if (hash_user && check_username_need_hash(user->host.str, user->host.length))
        {
            char new_user[130] = {0};
            char new_user_hash[17] = {0};
            sprintf(new_user, "%s_%s", user->user.str, user->host.str);
            md5_16(new_user_hash, new_user);
            str_append(sql, new_user_hash);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
        }
        else
        {
            str_append(sql, user->user.str);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
        }

        str_append(sql, "'");

        if (i < thd->lex->users_list.elements-1)
            str_append(sql, ",");
    }
    return false;
}

int get_create_user_sql(THD* thd, str_t* sql, bool hash_user)
{
    str_append(sql, "CREATE USER ");
    st_lex_user* user;
    for (uint i=0; i < thd->lex->users_list.elements; ++i)
    {
        user = thd->lex->users_list.elem(i);
        str_append(sql, "'");
        if (hash_user && check_username_need_hash(user->host.str, user->host.length))
        {
            char new_user[130] = {0};
            char new_user_hash[17] = {0};
            sprintf(new_user, "%s_%s", user->user.str, user->host.str);
            md5_16(new_user_hash, new_user);
            str_append(sql, new_user_hash);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
        }
        else
        {
            str_append(sql, user->user.str);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
        }

        str_append(sql, "' ");
        if (user->pwtext.length > 0)
        {
            str_append(sql, "IDENTIFIED BY '");
            str_append(sql, user->pwtext.str);
            str_append(sql, "'");
        }
        else if (user->pwhash.length > 0)
        {
            str_append(sql, "IDENTIFIED BY PASSWORD '");
            str_append(sql, user->pwhash.str);
            str_append(sql, "'");
        }
        else if (user->plugin.length > 0)
        {
            str_append(sql, "IDENTIFIED WITH ");
            str_append(sql, user->plugin.str);
            if (user->auth.length > 0)
            {
                str_append(sql, "AS ");
                str_append(sql, user->auth.str);
            }
        }

        if (i < thd->lex->users_list.elements-1)
            str_append(sql, ",");
    }
    
    if (thd->lex->ssl_type == SSL_TYPE_NONE)
    {
        str_append(sql, " REQUIRE NONE ");
    }
    else if (thd->lex->ssl_type == SSL_TYPE_ANY)
    {
        str_append(sql, " REQUIRE SSL ");
    }
    else if (thd->lex->ssl_type == SSL_TYPE_X509)
    {
        str_append(sql, " REQUIRE X509 ");
    }
    else if (thd->lex->ssl_type == SSL_TYPE_SPECIFIED)
    {
        int j = 0;
        if (thd->lex->x509_issuer)
        {
            str_append(sql, " REQUIRE CIPHER '");
            str_append(sql, thd->lex->x509_issuer);
            str_append(sql, "'");
            j = 1;
        }
        
        if (j == 1 && thd->lex->x509_subject)
        {
            str_append(sql, " AND SUBJECT '");
            str_append(sql, thd->lex->x509_subject);
            str_append(sql, "'");
        }
        else if (thd->lex->x509_subject)
        {
            str_append(sql, " REQUIRE SUBJECT '");
            str_append(sql, thd->lex->x509_subject);
            str_append(sql, "'");
            j = 1;
        }
        
        if (j == 1 && thd->lex->ssl_cipher)
        {
            str_append(sql, " AND CIPHER '");
            str_append(sql, thd->lex->ssl_cipher);
            str_append(sql, "'");
        }
        else if (thd->lex->ssl_cipher)
        {
            str_append(sql, " REQUIRE CIPHER '");
            str_append(sql, thd->lex->ssl_cipher);
            str_append(sql, "'");
            j = 1;
        }
    }
    
    if (thd->lex->mqh.specified_limits > 0)
    {
        str_append(sql, " WITH ");
        char tmp[50];
        if (thd->lex->mqh.specified_limits & user_resources::QUERIES_PER_HOUR)
        {
            sprintf(tmp, "%d", thd->lex->mqh.questions);
            str_append(sql, " MAX_QUERIES_PER_HOUR ");
            str_append(sql, tmp);
        }
        if (thd->lex->mqh.specified_limits & user_resources::UPDATES_PER_HOUR)
        {
            sprintf(tmp, "%d", thd->lex->mqh.updates);
            str_append(sql, " MAX_UPDATES_PER_HOUR ");
            str_append(sql, tmp);
        }
        if (thd->lex->mqh.specified_limits & user_resources::CONNECTIONS_PER_HOUR)
        {
            sprintf(tmp, "%d", thd->lex->mqh.conn_per_hour);
            str_append(sql, " MAX_CONNECTIONS_PER_HOUR ");
            str_append(sql, tmp);
        }
        if (thd->lex->mqh.specified_limits & user_resources::USER_CONNECTIONS)
        {
            sprintf(tmp, "%d", thd->lex->mqh.user_conn);
            str_append(sql, " MAX_USER_CONNECTIONS ");
            str_append(sql, tmp);
        }
    }
    
    //[password_option | lock_option] 这两个mysql中支持，mariadb这个版本不支持

    return false;
}

int get_alter_user_sql(THD* thd, str_t* sql, bool hash_user)
{
    str_append(sql, "ALTER USER ");

    if (thd->lex->create_info.if_exists())
    {
        str_append(sql, "IF EXISTS ");
    }
    
    st_lex_user* user;
    
    for (uint i=0; i < thd->lex->users_list.elements; ++i)
    {
        user = thd->lex->users_list.elem(i);
        if (hash_user && check_username_need_hash(user->host.str,user->host.length))
        {
            char new_user[130] = {0};
            char new_user_hash[17] = {0};
            sprintf(new_user, "%s_%s", user->user.str, user->host.str);
            md5_16(new_user_hash, new_user);
            str_append(sql, "'");
            str_append(sql, new_user_hash);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
            str_append(sql, "'");
        }
        else
        {
            str_append(sql, "'");
            str_append(sql, user->user.str);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
            str_append(sql, "'");
        }
        
        if (user->pwtext.length > 0)
        {
            str_append(sql, " IDENTIFIED BY '");
            str_append(sql, user->pwtext.str);
            str_append(sql, "'");
        }
        else if (user->plugin.length > 0)
        {
            str_append(sql, " IDENTIFIED WITH '");
            str_append(sql, user->plugin.str);
            str_append(sql, "'");
            if (user->auth.length > 0)
            {
                if (user->using_clause == 0)
                    str_append(sql, " USING '");
                else if (user->using_clause == 1)
                    str_append(sql, " AS '");
                else if (user->using_clause == 2)
                    str_append(sql, " BY '");
                str_append(sql, user->auth.str);
                str_append(sql, "'");
            }
        }
        
        if (i < thd->lex->users_list.elements-1)
        {
            str_append(sql, ", ");
        }
    }
    
    if (thd->lex->ssl_type == SSL_TYPE_NONE)
    {
        str_append(sql, " REQUIRE NONE ");
    }
    else if (thd->lex->ssl_type == SSL_TYPE_ANY)
    {
        str_append(sql, " REQUIRE SSL ");
    }
    else if (thd->lex->ssl_type == SSL_TYPE_X509)
    {
        str_append(sql, " REQUIRE X509 ");
    }
    else if (thd->lex->ssl_type == SSL_TYPE_SPECIFIED)
    {
        int j = 0;
        if (thd->lex->x509_issuer)
        {
            str_append(sql, " REQUIRE CIPHER '");
            str_append(sql, thd->lex->x509_issuer);
            str_append(sql, "'");
            j = 1;
        }
        
        if (j == 1 && thd->lex->x509_subject)
        {
            str_append(sql, " AND SUBJECT '");
            str_append(sql, thd->lex->x509_subject);
            str_append(sql, "'");
        }
        else if (thd->lex->x509_subject)
        {
            str_append(sql, " REQUIRE SUBJECT '");
            str_append(sql, thd->lex->x509_subject);
            str_append(sql, "'");
            j = 1;
        }
        
        if (j == 1 && thd->lex->ssl_cipher)
        {
            str_append(sql, " AND CIPHER '");
            str_append(sql, thd->lex->ssl_cipher);
            str_append(sql, "'");
        }
        else if (thd->lex->ssl_cipher)
        {
            str_append(sql, " REQUIRE CIPHER '");
            str_append(sql, thd->lex->ssl_cipher);
            str_append(sql, "'");
            j = 1;
        }
    }
    
    if (thd->lex->mqh.specified_limits > 0)
    {
        str_append(sql, " WITH ");
        char tmp[50];
        if (thd->lex->mqh.specified_limits & user_resources::QUERIES_PER_HOUR)
        {
            sprintf(tmp, "%d", thd->lex->mqh.questions);
            str_append(sql, " MAX_QUERIES_PER_HOUR ");
            str_append(sql, tmp);
        }
        if (thd->lex->mqh.specified_limits & user_resources::UPDATES_PER_HOUR)
        {
            sprintf(tmp, "%d", thd->lex->mqh.updates);
            str_append(sql, " MAX_UPDATES_PER_HOUR ");
            str_append(sql, tmp);
        }
        if (thd->lex->mqh.specified_limits & user_resources::CONNECTIONS_PER_HOUR)
        {
            sprintf(tmp, "%d", thd->lex->mqh.conn_per_hour);
            str_append(sql, " MAX_CONNECTIONS_PER_HOUR ");
            str_append(sql, tmp);
        }
        if (thd->lex->mqh.specified_limits & user_resources::USER_CONNECTIONS)
        {
            sprintf(tmp, "%d", thd->lex->mqh.user_conn);
            str_append(sql, " MAX_USER_CONNECTIONS ");
            str_append(sql, tmp);
        }
    }

    //[password_option | lock_option] 这两个mysql中支持，mariadb这个版本不支持
    
    return false;
}

int get_rename_user_sql(THD* thd, str_t* sql, bool hash_user)
{
    str_append(sql, "RENAME USER ");
    
    st_lex_user* user1;
    st_lex_user* user2;
    
    for (uint i=0; i+1 < thd->lex->users_list.elements;)
    {
        user1 = thd->lex->users_list.elem(i);
        user2 = thd->lex->users_list.elem(i+1);
        
        if (hash_user && check_username_need_hash(user1->host.str, user1->host.length))
        {
            char new_user[130] = {0};
            char new_user_hash[17] = {0};
            sprintf(new_user, "%s_%s", user1->user.str, user1->host.str);
            md5_16(new_user_hash, new_user);
            str_append(sql, "'");
            str_append(sql, new_user_hash);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
            str_append(sql, "' TO '");
            memset(new_user, 0, 130);
            memset(new_user_hash, 0, 17);
            sprintf(new_user, "%s_%s", user2->user.str, user2->host.str);
            md5_16(new_user_hash, new_user);
            str_append(sql, new_user_hash);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
            str_append(sql, "'");
        }
        else
        {
            str_append(sql, "'");
            str_append(sql, user1->user.str);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
            str_append(sql, "' TO '");
            str_append(sql, user2->user.str);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
            str_append(sql, "'");
        }
        
        if (i+1 < thd->lex->users_list.elements-1)
        {
            str_append(sql, ", ");
        }
        
        i = i + 2;
    }
    
    return false;
}

int get_show_create_user_sql(THD* thd, str_t* sql, bool hash_user)
{
    str_append(sql, "SHOW CREATE USER ");
    char* hostname;
    char* username;
    
    if (thd->lex->grant_user)
    {
        if (thd->lex->grant_user->user.str == current_user.str)
        {
            str_append(sql, "CURRENT_USER()");
            return false;
        }
        if (thd->lex->grant_user->user.str == current_user_and_current_role.str)
        {
            hostname = thd->security_ctx->priv_host;
            username = thd->security_ctx->priv_user;
        }
        else
        {
            hostname = thd->lex->grant_user->host.str;
            username = thd->lex->grant_user->user.str;
        }

        if (hostname && hash_user && check_username_need_hash(hostname, strlen(hostname)))
        {
            char new_user[130] = {0};
            char new_user_hash[17] = {0};
            sprintf(new_user, "%s_%s", username, hostname);
            md5_16(new_user_hash, new_user);
            str_append(sql, "'");
            str_append(sql, new_user_hash);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);
            str_append(sql, "'");
        }
        else
        {
            str_append(sql, "'");
            str_append(sql, username);
            str_append(sql, "'@'");
            str_append(sql, proxy_server_address);

            str_append(sql, "'");
        }
    }
    return false;
}

int get_show_grants_sql(THD* thd, str_t* sql, bool hash_user)
{
    str_append(sql, "SHOW GRANTS ");
    char* hostname;
    char* username;
    
    if (thd->lex->grant_user)
    {
        if (thd->lex->grant_user->user.str == current_user_and_current_role.str)
        {
            hostname = thd->security_ctx->priv_host;
            username = thd->security_ctx->priv_user;
            str_append(sql, "FOR ");
            str_append(sql, "'");
            str_append(sql, username);
            str_append(sql, "'@'");
            /* show the user should use the priv host name, include % etc. 
             * but not the proxy_server_address, because login can use the 
             * not encypted username with other hostname, so it is prossible 
             * not the proxy_server_address */
            //str_append(sql, proxy_server_address);
            str_append(sql, hostname);
            str_append(sql, "'");
        }
        else if (thd->lex->grant_user->user.str == current_user.str)
        {
            str_append(sql, "FOR CURRENT_USER()");
        }
        else
        {
            str_append(sql, "FOR ");
            hostname = thd->lex->grant_user->host.str;
            username = thd->lex->grant_user->user.str;
            if (hostname && hash_user && check_username_need_hash(hostname, strlen(hostname)))
            {
                char new_user[130] = {0};
                char new_user_hash[17] = {0};
                sprintf(new_user, "%s_%s", username, hostname);
                md5_16(new_user_hash, new_user);
                str_append(sql, "'");
                str_append(sql, new_user_hash);
                str_append(sql, "'@'");
                str_append(sql, proxy_server_address);
                str_append(sql, "'");
            }
            else
            {
                str_append(sql, "'");
                str_append(sql, username);
                str_append(sql, "'@'");
                /* show the user should use the priv host name, include % etc. 
                 * but not the proxy_server_address, because login can use the 
                 * not encypted username with other hostname, so it is prossible 
                 * not the proxy_server_address */
                //str_append(sql, proxy_server_address);
                if (hostname)
                    str_append(sql, hostname);
                else
                    str_append(sql, "%");
                str_append(sql, "'");
            }
        }
    }

    return false;
}

int get_set_var_sql(THD* thd, set_var_base *var, format_cache_node_t* format_node)
{
    if (!strcasecmp(var->set_var_type, "set_var"))
    {
        set_var* s_var = dynamic_cast <set_var*> (var);
        str_append(format_node->format_sql, "set ");
        str_append(format_node->format_sql, s_var->var->name.str);

        str_append(format_node->format_sql, "=");
        if (!s_var->value)
            str_append(format_node->format_sql, "DEFAULT");
        else
            format_item(thd, format_node, format_node->format_sql, 
                s_var->value, &thd->lex->select_lex, false);
    }
    else if (!strcasecmp(var->set_var_type, "set_user_var"))
    {
        set_var_user* s_var = dynamic_cast <set_var_user*> (var);
        if (s_var->get_var_user()
            && s_var->get_var_user()->arguments()
            && s_var->get_var_user()->arguments()[0])
        {
            str_append(format_node->format_sql, "set @");
            str_append(format_node->format_sql, s_var->get_var_user()->name.str);
            str_append(format_node->format_sql, "=");
            format_item(thd, format_node, format_node->format_sql,
                        s_var->get_var_user()->arguments()[0], &thd->lex->select_lex, false);
        }
    }
    return false;
}

int mysql_format_set_var(THD* thd, format_cache_node_t* format_node)
{
    char* dupcharsql;
    str_append_with_length(format_node->sql_statements, thd->query(), thd->query_length());
    
    set_var_base *var = thd->lex->var_list.pop();
    do
    {
        get_set_var_sql(thd, var, format_node);
        if (!thd->lex->var_list.is_empty())
        {
            str_append(format_node->format_sql, ", ");
        }
    }while ((var=thd->lex->var_list.pop()));
    
    dupcharsql = (char*)my_malloc(str_get_len(format_node->format_sql) * 2 + 1, MYF(0));
    memset(dupcharsql, 0, str_get_len(format_node->format_sql) * 2 + 1);
    mysql_dup_char_with_len(str_get(format_node->format_sql), dupcharsql, '\'',
                            str_get_len(format_node->format_sql));
    str_truncate_0(format_node->format_sql);
    str_append_with_length(format_node->format_sql, dupcharsql, strlen(dupcharsql));

    my_free(dupcharsql);
    return false;
}

int assemble_other_info(THD* thd, format_cache_node_t* format_node)
{
    if (thd->security_ctx)
    {
        if (thd->security_ctx->host_or_ip)
        {
            //strcpy(format_node->hostname, thd->security_ctx->host);
            strcpy(format_node->client_ip, thd->security_ctx->ip);
        }

        if (thd->security_ctx->user)
        {
            strcpy(format_node->username, thd->security_ctx->external_user == NULL
                ? thd->security_ctx->user : thd->security_ctx->external_user);
        }
    }

    if (thd->db)
        strcpy(format_node->dbname, thd->db);

    return false;
}

int mysql_format_command(THD *thd, format_cache_node_t* format_node)
{
    int err;
    
    switch (thd->lex->sql_command)
    {
        case SQLCOM_CHANGE_DB:
            err = mysql_format_change_db(thd, format_node);
            break;
        case SQLCOM_SET_OPTION:
            err = mysql_format_set(thd, format_node);
            break;
        case SQLCOM_INSERT:
        case SQLCOM_INSERT_SELECT:
            err = mysql_format_insert(thd, format_node);
            break;
            
        case SQLCOM_DELETE:
        case SQLCOM_DELETE_MULTI:
            err = mysql_format_delete(thd, format_node);
            break;
            
        case SQLCOM_UPDATE:
        case SQLCOM_UPDATE_MULTI:
            err = mysql_format_update(thd, format_node);
            break;
            
        case SQLCOM_SELECT:
            err = mysql_format_select(thd, format_node);
            break;
            
        case SQLCOM_GRANT:
            err = mysql_format_grant(thd, format_node);
            break;
        
        case SQLCOM_REVOKE:
        case SQLCOM_REVOKE_ALL:
            err = mysql_format_revoke(thd, format_node);

        default:
            err = mysql_format_others(thd, format_node);
            //err = mysql_format_not_support(thd, format_node);
            break;
    }

    assemble_other_info(thd, format_node);
    return err;
}
