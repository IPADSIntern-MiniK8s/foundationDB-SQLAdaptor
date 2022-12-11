import sys
sys.path.append("..")

from repository import HeaderRepo
from repository import GeoRepo
from sqlparser import ExecutionBuilder, Ops
from basic_ops import TABLE_NAMES


class ClientService(object):
    def __init__(self):
        self.header_repo = HeaderRepo()
        self.geo_repo = GeoRepo()
        self.execution_builder = ExecutionBuilder()


    '''
    :execute 'select' instruction
    :NOTE: now only support one format: 'SELECT * from header WHERE time_stamp > x and time_stamp < y'
    :return: a list of query result
    '''
    def execute_select(self, plan):
        if not plan or len(plan) == 0:
            return
        target_table = plan['table_name'][0]
        columns = plan['columns']
        lower_bound = None
        higher_bound = None
        
        


    def query_by_sql(self, raw_sql):
        plans = self.execution_builder.generate_execution(raw_sql)
        