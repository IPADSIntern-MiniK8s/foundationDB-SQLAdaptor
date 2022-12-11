import sys

sys.path.append("..")

import sqlparse
from sqlparser.token_parser import TokenParser, Ops
  

class ExecutionBuilder(object):
    def __init__(self):
        return
    
    '''
    :split mutiple sql setences to a list
    :a list of statements 
    '''
    def preprocess(self, raw_sql):
        statements = sqlparse.split(raw_sql)
        return statements


    '''
    :generate executing plan for 'SELECT'
    :for example: SELECT column1, column2, ... FROM table_name WHERE condition;
    :return: a dict(empty means fail)
    '''
    def handle_select(self):
        plan = dict()

        # only single table selection is supported
        if len(self.tables) != 1:
            return dict()
        plan['type'] = self.query_type
        plan['table_name'] = self.tables
        
        # check wanted columns
        if 'select' not in self.columns_dict:
            return dict()
        plan['columns'] = self.columns_dict['select']

        # check the condition
        if len(self.exps) != 0:
            assert len(self.union_ops) == len(self.exps) - 1
        plan['conditions'] = self.exps
        plan['union_ops'] = self.union_ops

        return plan

    '''
    :generate executing plan for 'INSERT INTO'
    :for example: INSERT INTO table_name VALUES (value1,value2,value3,...);
    :return: a dict(empty means fail)
    '''
    def handle_insert(self):
        plan = dict()

        # only single table insertion is supported
        if len(self.tables) != 1:
            return dict()
        plan['type'] = self.query_type
        plan['table_name'] = self.tables

        # check wanted columns
        if 'insert' not in self.columns_dict:
            return dict()
        
        # check the values
        plan['values'] = self.value_dict

        return plan

    '''
    :generate executing plan for 'UPDATE'
    :for example: UPDATE table_name SET column1 = value1, column2 = value2, ... WHERE condition;
    :return: a dict(empty means fail)
    '''
    def handle_update(self):
        plan = dict()

        # only single table update is supported
        if len(self.tables) != 1:
            return dict()
        plan['type'] = self.query_type
        plan['table_name'] = self.tables

        # the number of columns and values to be updated should be equal
        if len(self.columns_dict['update']) != len(self.value_dict):
            return dict()
        
        plan['columns'] = self.columns_dict['update']
        plan['values'] = self.value_dict
        plan['conditions'] = self.exps
        plan['union_ops'] = self.union_ops

        return plan

    '''
    :generate executing plan for 'DELETE'
    :for example: DELETE FROM table_name WHERE condition;
    :return: a dict(empty means fail)
    '''
    def handle_delete(self):
        plan = dict()

        # only single table deletion is supported
        if len(self.tables) != 1:
            return dict()
        plan['type'] = self.query_type
        plan['table_name'] = self.tables
        plan['conditions'] = self.exps
        plan['union_ops'] = self.union_ops

        return plan

    

    '''
    :generate execution plan
    :return a list execution info
    '''
    def generate_execution(self, raw_sql):
        statements = self.preprocess(raw_sql)
        token_parser = TokenParser()
        result = list()
        for state in statements:
            self.query_type, self.tables, self.columns_dict, self.value_dict, self.exps, self.union_ops = token_parser.parse_token(state)
            plan = dict()

            if self.query_type == 'SELECT':
                plan = self.handle_select()
            elif self.query_type == 'INSERT':
                plan = self.handle_insert()
            elif self.query_type == 'UPDATE':
                plan = self.handle_update()
            elif self.query_type == 'DELETE':
                plan = self.handle_delete()
            
            print('execution plan: ', plan)
            result.append(plan)
        
        return result

    
        
    


