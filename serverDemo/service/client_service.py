from msilib.schema import Complus
import sys
sys.path.append("..")

from repository import HeaderRepo
from repository import GeoRepo, ImgRepo
from sqlparser import ExecutionBuilder, Ops
from basic_ops import TABLE_NAMES


class ClientService(object):
    def __init__(self):
        self.header_repo = HeaderRepo()
        self.geo_repo = GeoRepo()
        self.img_repo = ImgRepo()
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
        if len(plan['columns']) == 1 and plan['columns'][0] == '*':  # need to pick all columns
            columns = None
        lower_bound = None
        higher_bound = None
        
        i = 0
        data_after_query = None
        # don't have condition
        # return `data_after_query`'s type is list of target object
        if 'condition' not in plan:
            if target_table == 'header':
                data_after_query = self.header_repo.find_all()
            elif target_table == 'geometry':
                data_after_query = self.geo_repo.find_by_message_id_range()
            elif target_table == 'picture':
                data_after_query = self.img_repo.find_by_message_id_range()




    def query_by_sql(self, raw_sql):
        plans = self.execution_builder.generate_execution(raw_sql)
        