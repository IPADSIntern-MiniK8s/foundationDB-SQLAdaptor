import sys
sys.path.append("..")

from sql_metadata import Parser
import sqlparse
from enum import Enum


class Ops(Enum):
    AND_OP = '&'
    OR_OP = '|'
    EQ_OP = '='
    LE_OP = '<='
    LT_OP = '<'
    GE_OP = '>='
    GT_OP = '>'
    PLUS_OP = '+'
    MINUS_OP = '-'
    TIMES_OP = '*'
    DIVIDE_OP = '/'


class TokenParser(object):
    def __init__(self):
        self.stack = list()

    '''
    :calculate the value according to expression tree
    :return: expression total value
    '''
    def calculate_exp(self, tree):
        stack = list()
        token_count = len(tree.tokens)
        for idx, token in enumerate(tree.tokens):
            cls = token._get_repr_name()
            value = str(token)

            if cls == 'Punctuation' or cls == 'Whitespace':
                continue
            if not token.is_group:
                stack.append((cls, value))
            else:
                value = self.calculate_exp(token)
                stack.append(('Float', value))  # unified as a floating point number
            
        index = len(stack) - 1
        tmp_value = 0
        while index >= 2:
            # TODO: need to check the left value and right value's class
            right_cls, right_value = stack[-1]
            right_value = eval(right_value)
            stack.pop()
            op_cls, op_value = stack[-1]
            assert op_cls == 'Operator'
            stack.pop()
            left_cls, left_value = stack[-1]
            left_value = eval(left_value)
            stack.pop()
           
            # calculate the result
            if op_value == Ops.PLUS_OP.value:
                tmp_value = left_value + right_value
            elif op_value == Ops.MINUS_OP.value:
                tmp_value = left_value - right_value
            elif op_value == Ops.TIMES_OP.value:
                tmp_value = left_value * right_value
            elif op_value == Ops.DIVIDE_OP.value:
                tmp_value = left_value / right_value
            stack.append((left_cls, str(tmp_value)))
            index = len(stack) - 1

        assert len(stack) == 1
        return_cls, return_value = stack[-1]
        return return_value


    '''
    :format comparsion to [identifier, cmp_op, right_value]
    :return: a list
    '''
    def format_comparsion(self, tree):
        result = list()
        
        for idx, token in enumerate(tree.tokens):
            cls = token._get_repr_name()
            value = str(token)
            if cls == 'Whitespace':
                continue
            elif cls == 'Identifier' or cls == 'Comparison':
                result.append(value)
                continue
            elif not token.is_group:
                result.append(value)
            else:
                value = self.calculate_exp(token)
                result.append(value)
            
        return result


    '''
    :parse the condition expression after `where`
    :return exp list and union ops
    '''
    def parse_exp(self, sql):
        statements = sqlparse.parse(sql)
        # traverse to find exp after `where`
        exp_list = list()
        union_ops = list()
        for token in statements[0].tokens:
            # for example: 
            # for "Select * from emp where sal > 2000 AND sal < (3000 - 5);", the `while` subsentence is `where sal > 2000 AND sal < (3000 - 5);`
            if type(token) == sqlparse.sql.Where:
                # token._pprint_tree()
                for idx, token in enumerate(token.tokens):
                    cls = token._get_repr_name()
                    value = str(token)

                    if cls == 'Keyword':
                        tmp_value = value.upper()
                        if tmp_value == 'AND':
                            union_ops.append(Ops.AND_OP)
                        elif tmp_value == 'OR':
                            union_ops.append(Ops.OR_OP)
                        continue

                    if token.is_group and (cls == 'Comparison'):
                        cmp_exp = self.format_comparsion(tree=token)
                        exp_list.append(cmp_exp)

        return exp_list, union_ops


    '''
    :parse the value that the update statement wants to reset
    :return: the column's according value dict (empty means fail)
    '''
    def parse_set_values(self, sql, column_list):
        statements = sqlparse.parse(sql)
        sql_len = len(sql)
        # statements[0]._pprint_tree()
        value_list = dict()

        for column in column_list:
            pos = sql.find(column)
            # not found, something wrong
            if pos == -1:
                return list()
            # not found, something wrong
            eq_pos = sql.find('=', pos)
            if eq_pos == -1:
                return list()

            # find the according value
            cur = eq_pos + 1
            while cur < sql_len and sql[cur] == ' ':
                cur += 1
            
            if cur == sql_len:
                value_list[column] = 'null'
            else: 
                end_pos = sql.find(' ', pos)
                value = sql[cur: end_pos]
                # if has '' or "" or , , remove it
                value = value.strip(',')
                value = value.strip('\'')
                value = value.strip('\"')
                value_list[column] = value

        return value_list
                    
            
    def parse_token(self, sql):
        exps, union_ops = self.parse_exp(sql=sql)
        parser = Parser(sql)
        # select, insert, update, replace, create table, alter table, with + select
        query_type = parser.query_type
        # dict with lists of columns divided into select, where, order_by, group_by, join, insert and update
        # it's resolved to actual columns, not alias
        columns_dict = parser.columns_dict
        tables = parser.tables
        value_dict = parser.values_dict
        if query_type == "UPDATE":
            value_dict = self.parse_set_values(sql, columns_dict['update'])

        return query_type, tables, columns_dict, value_dict, exps, union_ops

    
    '''
    :print token parser result (for debug)
    '''
    def token_parser_output(self, sql):
        query_type, tables, columns_dict, value_dict, exps, union_ops = self.parse_token(sql)
        print('query_type: ', query_type)
        print('tables: ', tables)
        print('columns_dict: ', columns_dict)
        print('value_dict: ', value_dict)
        print('exp: ', exps)
        print('union ops: ', union_ops)



