from enum import Flag
import sys
sys.path.append("..")

from repository import HeaderRepo
from repository import GeoRepo, ImgRepo
from sqlparser import ExecutionBuilder, Ops
from basic_ops import TABLE_NAMES
from entity.geo_data import GEO_COLUMNS
from entity.header_data import HEADER_COLUMNS
from entity.img_data import IMG_COLUMNS



class ClientService(object):
    def __init__(self):
        self.header_repo = HeaderRepo()
        self.geo_repo = GeoRepo()
        self.img_repo = ImgRepo()
        self.execution_builder = ExecutionBuilder()


    '''
    :intersection of two lists according to message_id
    :return: a new list of object
    '''
    def intersection(self, list1, list2):
        result = list()
        list1 = sorted(list1)
        list2 = sorted(list2)
        i = 0
        j = 0
        size1 = len(list1)
        size2 = len(list2)
        while i < size1 and j < size2:
            if list1[i].message_id == list2[j].message_id:
                result.append(list1[i])
                i += 1
                j += 1
            elif list1[i].message_id < list2[j].message_id:
                i += 1
            else:
                j += 1
        return result


    '''
    :union of two lists according to message_id
    :return: a new list of object
    '''
    def union(self, list1, list2):
        result = list()
        list1 = sorted(list1)
        list2 = sorted(list2)
        i = 0
        j = 0
        size1 = len(list1)
        size2 = len(list2)
        while i < size1 and j < size2:
            if list1[i].message_id == list2[j].message_id:
                result.append(list1[i])
                i += 1
                j += 1
            elif list1[i].message_id < list2[j].message_id:
                result.append(list1[i])
                i += 1
            else:
                result.append(list2[j])
                j += 1
        if i < size1:
            result.extend(list1[i:])
        if j < size2:
            result.extend(list2[j:])
        return result


    '''
    :check the updated value and its position for update sql, if not update, fill with False
    :return: a list
    '''
    def check_update(self, COLUMNS, values):
        updated = list()
        for column in COLUMNS:
            if column in values:
                updated.append(values[column])
            else:
                updated.append(False)
        return updated
                

    '''
    :execute 'select' and 'delete' instruction
    :NOTE: not support `join`
    :delete format: DELETE FROM table name WHERE column name = value
                    DELETE FROM table name WHERE column name > value
                    ...
    :return: a list of query result
    '''
    def execute_select_delete(self, plan):
        if not plan or len(plan) == 0:
            return list()
        target_table = plan['table_name'][0]
        columns = None
        if plan['type'] == 'SELECT' and not(len(plan['columns']) == 1 and plan['columns'][0]) == '*':  # need to pick all columns
            columns = plan['columns']
        
        data_after_query = None

        # don't have condition
        # return `data_after_query`'s type is list of target object
        if 'conditions' not in plan or len(plan['conditions']) == 0:
            if target_table == 'header':
                data_after_query = self.header_repo.find_all(columns)
            elif target_table == 'geometry':
                data_after_query = self.geo_repo.find_by_message_id_range(columns)
            elif target_table == 'picture':
                data_after_query = self.img_repo.find_by_message_id_range(columns)

            return data_after_query
        
        # handle the `where` clause
        # NOTE: not consider priority when using conditions
        i = 0
        conditions = plan['conditions']
        union_ops = plan['union_ops']
        condition_size = len(conditions)
        union_op_size = len(union_ops)

        assert union_op_size == condition_size - 1 or union_op_size == 0

        while i < condition_size:
            attribute = conditions[i][0]
            op = conditions[i][1]
            val = eval(conditions[i][2])    # NOTE: here if val = img, error happens
            other_attribute = conditions[i][0]
            other_op = None
            other_val = None
            i += 1
            # if union_op_size > 0 and i < condition_size:
            #     print(union_op_size, i, condition_size, union_ops[i - 1], conditions[i][0], other_attribute)
            if union_op_size > 0 and i < condition_size and union_ops[i - 1] == Ops.AND_OP and conditions[i][0] == other_attribute:
                other_op = conditions[i][1]
                other_val = eval(conditions[i][2])
                i += 1
            
            # satisfy left-open and right-close, val is the lower bound, other_val is the upper bound
            # for '='
            if op == Ops.EQ_OP.value and other_op == None:
                other_val = val + 1

            # for ('<'/'<=', '>'/'>='), swap val and other_val
            if op == Ops.LE_OP.value or op == Ops.LT_OP.value or other_op == Ops.GE_OP.value or other_op == Ops.GT_OP.value:
                val, other_val = other_val, val
                op, other_op = other_op, op

            # the left op must be '>='
            if op == Ops.GT_OP.value:
                val += 1
            # the right op must be '<'
            if other_op == Ops.LE_OP.value:
                other_val += 1 

            # FIXME: for timestamp range query bug
            # if other_op == Ops.LE_OP.value and other_attribute == 'time_stamp':
            #     other_val += 1
            
            # NOTE: only a few cases with corresponding APIs are enumerated
            # TODO: maybe need to supply
            data = None
            if target_table == 'header':
                if op == Ops.EQ_OP.value and attribute == 'message_id' and plan['type'] == 'SELECT':
                    data = self.header_repo.find_by_message_id(val, columns)
                elif attribute == 'message_id' and plan['type'] == 'SELECT':
                    data = self.header_repo.find_by_message_id_range(columns, val, other_val)
                elif op == Ops.EQ_OP.value and attribute == 'message_id' and plan['type'] == 'DELETE':
                    data = self.header_repo.del_by_message_id(val)
                elif attribute == 'message_id' and plan['type'] == 'DELETE':
                    data = self.header_repo.del_by_message_id_range(val, other_val)
                elif op == Ops.EQ_OP.value and attribute == 'time_stamp':
                    data = self.header_repo.find_by_timestamp(str(val), columns)
                elif attribute == 'time_stamp':
                    data = self.header_repo.find_by_timestamps_range(columns, str(val), str(other_val))
                elif op == Ops.EQ_OP.value and attribute == 'car_id':
                    data = self.header_repo.find_by_car_id(val, columns)
            elif target_table == 'geometry':
                if op == Ops.EQ_OP.value and attribute == 'message_id':
                    data = self.geo_repo.find_by_message_id(val, columns)
                elif attribute == 'message_id':
                    data = self.geo_repo.find_by_message_id_range(val, columns)
                else:
                    data = self.geo_repo.find_by_unindex_attribute_range(attribute, columns, val, other_val)
            elif target_table == 'picture':
                if op == Ops.EQ_OP.value and attribute == 'message_id':
                    data = self.img_repo.find_by_message_id(val, columns)
                elif attribute == 'message_id':
                    data = self.img_repo.find_by_message_id_range(columns, val, other_val)
            
            # union or insert with former conditions
            if not isinstance(data, list):
                data = [data]
            if data_after_query == None:
               data_after_query = data
            elif i >= 2 and union_ops[i - 2] == Ops.AND_OP:
                data_after_query = self.intersection(data_after_query, data)
            elif i >= 2 and union_ops[i - 2] == Ops.OR_OP:
                data_after_query = self.union(data_after_query, data)
        return data_after_query   


    '''
    :execute 'insert' instruction (which don't have `where` clause)
    :format: "INSERT INTO Websites (name, url, alexa, country) VALUES ('百度','https://www.baidu.com/','4','CN');"
    :NOTE: message_id(primary key) must be specified
    :return: SUCCESS(True) or FAIL(False)
    '''
    def execute_insert(self, plan):
        if not plan or len(plan) == 0:
            return False

        target_table = plan['table_name'][0]
        if target_table not in TABLE_NAMES:
            return False
        
        if 'values' not in plan:
            return False

        values = plan['values']

        if target_table == 'header':
            if len(values) != len(HEADER_COLUMNS):
                return False
            if HEADER_COLUMNS[0] not in values:   # miss primary key
                return False
            message_id = values[HEADER_COLUMNS[0]]
            time_stamp = values.get(HEADER_COLUMNS[1], None)
            car_id = values.get(HEADER_COLUMNS[2], None)
            ret = self.header_repo.save(message_id, time_stamp, car_id)
            return ret
        elif target_table == 'geometry':
            if len(values) != len(GEO_COLUMNS):
                return False
            if GEO_COLUMNS[0] not in values:  # miss primary key
                return False
            message_id = values[GEO_COLUMNS[0]]
            x = values.get(GEO_COLUMNS[1], None)
            y = values.get(GEO_COLUMNS[2], None)
            v_x = values.get(GEO_COLUMNS[3], None)
            v_y = values.get(GEO_COLUMNS[4], None)
            v_r = values.get(GEO_COLUMNS[5], None)
            direction = values.get(GEO_COLUMNS[6], 0)
            ret = self.geo_repo.save(message_id, x, y, v_x, v_y, v_r, direction)
            return ret
        elif target_table == 'picture':
            if len(values) != len(IMG_COLUMNS):
                return False
            if IMG_COLUMNS[0] not in values:
                return False
            message_id = values[IMG_COLUMNS[0]]
            img = values.get(IMG_COLUMNS[1], None)
            ret = self.img_repo.save(message_id, img)
            return ret

        return False


    '''
    :execute 'insert' instruction (which don't have `where` clause)
    :format: "UPDATE table name SET column name=new value WHERE column name=value;"
    :return: a list of updated entries
    '''
    def execute_update(self, plan):
        # first find the entries that satisfy the `where` clause
        search_plan = plan
        search_plan['columns'] = ['*']
        search_result = self.execute_select_delete(search_plan)
        if not isinstance(search_result, list):
            return list()
        
        target_table = plan['table_name'][0]
        if target_table not in TABLE_NAMES:
            return list()
        
        # no value to update
        if 'values' not in plan:
            return list()
        values = plan['values']

        result = list()
        if target_table == 'header':
            updated = self.check_update(HEADER_COLUMNS, values)
            for entry in search_result:
                if entry == None:
                    continue
                if updated[0] != False:
                    entry.message_id = eval(updated[0])
                if updated[1] != False:
                    entry.time_stamp = updated[1]
                if updated[2] != False:
                    entry.car_id = eval(updated[2])
                ret = self.header_repo.save(entry.message_id, entry.time_stamp, entry.car_id)
                if not ret:
                    result.append(False)
                else:
                    result.append(entry)
            return result
        elif target_table == 'geometry':
            updated = self.check_update(GEO_COLUMNS, values)
            for entry in search_result:
                if entry == None:
                    continue
                if updated[0] != False:
                    entry.message_id = eval(updated[0])
                if updated[1] != False:
                    entry.x = eval(updated[1])
                if updated[2] != False:
                    entry.y = eval(updated[2])
                if updated[3] != False:
                    entry.v_x = eval(updated[3])
                if updated[4] != False:
                    entry.v_y = eval(updated[4])
                if updated[5] != False:
                    entry.v_r = eval(updated[5])
                if updated[6] != False:
                    entry.direction = eval(updated[6])
                ret = self.geo_repo.save(entry.message_id, entry.x, entry.y, entry.v_x, entry.v_y, entry.v_r, entry.direction)
                if not ret:
                    result.append(False)
                else:
                    result.append(entry)
            return result
        elif target_table == 'picture':
            updated = self.check_update(IMG_COLUMNS, values)
            for entry in search_result:
                if entry == None:
                    continue
                if updated[0] != False:
                    entry.message_id = eval(updated[0])
                if updated[1] != False:
                    entry.img = updated[1]
                ret = self.img_repo.save(entry.message_id, entry.img)
                if not ret:
                    result.append(False)
                else:
                    result.append(entry)
            return result
        
        return list()


    def query_by_sql(self, raw_sql):
        plans = self.execution_builder.generate_execution(raw_sql)
        result = list()
        for plan in plans:
            if plan['type'] == 'SELECT' or plan['type'] == 'DELETE':
                result.extend(self.execute_select_delete(plan))
            elif plan['type'] == 'INSERT':
                result.append(self.execute_insert(plan))
            elif plan['type'] == 'UPDATE':
                result.extend(self.execute_update(plan))

        return result