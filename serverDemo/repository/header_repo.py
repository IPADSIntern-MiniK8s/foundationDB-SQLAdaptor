import sys
sys.path.append("..")

from entity import HeaderData
from basic_ops import FdbTool
from entity.header_data import HEADER_COLUMNS


TABLE_NAME = 'header'
TIME_INDEX_NAME = 'time_index'
CAR_INDEX_NAME = 'car_index'


class HeaderRepo(object):
    def __init__(self):
        self.fdb_tool = FdbTool()
    
    
    '''
    :save the data and index
    '''
    def save(self, message_id, time_stamp, car_id):
        header_data = HeaderData(message_id, time_stamp, car_id)
        # save header data
        key, value = header_data.encode_data()
        ret = self.fdb_tool.add(self.fdb_tool.db, TABLE_NAME, key, value)
        assert ret == True
        # save index
        time_key, time_value, car_key, car_value = header_data.encode_index()
        ret = self.fdb_tool.add(self.fdb_tool.db, TIME_INDEX_NAME, time_key, time_value)
        assert ret == True
        ret = self.fdb_tool.add(self.fdb_tool.db, CAR_INDEX_NAME, car_key, car_value)
        assert ret == True
        return ret


    '''
    :auxiliary function: check which column to choose
    :return: HeaderData 
    '''
    def check_column(self, key, value, column):
        header_data = HeaderData()
        header_data.message_id = key
        if column == None or HEADER_COLUMNS[1] in column:
            header_data.time_stamp = value[0]
        else:
            header_data.time_stamp = None
        if column == None or HEADER_COLUMNS[2] in column:
            header_data.car_id = value[1]
        else:
            header_data.car_id = None
        return header_data


    '''
    :find header data entry in database by message id
    :return: HeaderData
    '''
    def find_by_message_id(self, message_id, column):
        data = self.fdb_tool.query(self.fdb_tool.db, TABLE_NAME, message_id)
        header_data = HeaderData()
        if data == None or len(data) == 0:
            return header_data

        header_data = self.check_column(message_id, data, column)
        return header_data


    def find_by_message_id_range(self, column, lower_message_id=None, upper_message_id=None):
        result = list()
        data = self.fdb_tool.query_range(self.fdb_tool.db, TABLE_NAME, (lower_message_id, ''), (upper_message_id, ''))

        if data == None or len(data) == 0:
            return result
        
        for key, value in data.items():
            header_data = self.check_column(key[0], value, column)
            result.append(header_data)
        return result
    

    '''
    : find all header data entry
    : return: a list of data entry
    '''
    def find_all(self, column):
        result = list()
        data = self.fdb_tool.query_all(self.fdb_tool.db, TABLE_NAME)
        if data == None or len(data) == 0:
            return data
        for key, value in data.items():
            header_data = self.check_column(key, value, column)
            result.append(header_data)
        return result


    '''
    :find header data entry in database by timestamp
    :return: a list of HeaderData(empty means fail)
    '''
    def find_by_timestamp(self, timestamp, column):
        data = self.fdb_tool.query_condition_all(self.fdb_tool.db, TIME_INDEX_NAME, timestamp)
        result = list()
        if data == None or len(data) == 0:
            return result
        
        for elem in data:
            # the structure of index: (time_stamp, message_id)
            header_data = HeaderData()
            entry = self.fdb_tool.query(self.fdb_tool.db, TABLE_NAME, elem[1])
            if entry != None:
                header_data = self.check_column(elem[1], entry, column)
                result.append(header_data)
        return result


    '''
    :find header data entries in database by timestamp in a range
    :return: a list of HeaderData
    '''
    def find_by_timestamps_range(self, column, lower_bound=None, upper_bound=None):
        lower_val = None
        upper_val = None
        if lower_bound != None:
            lower_val = (lower_bound, '')
        if upper_bound != None:
            upper_val = (upper_bound, '\xFF')
        
        # find the timestamps and their according message id
        data = self.fdb_tool.query_range(self.fdb_tool.db, TIME_INDEX_NAME, lower_val, upper_val)
        result = list()

        # find message_id's according header data
        data_len = len(data)
        if data_len == 0:
            print('the first step of timestamp is empty')
            return result
        for elem in data:
            message_id = elem[1]
            header = self.fdb_tool.query(self.fdb_tool.db, TABLE_NAME, message_id)
            if header != None:
                header_data = self.check_column(message_id, header, column)
                result.append(header_data)
        
        return result

    
    '''
    :find header data entry in database by carId
    :return: a list of header_data or empty list
    '''
    def find_by_car_id(self, car_id, column):
        indexes = self.fdb_tool.query_condition_all(self.fdb_tool.db, CAR_INDEX_NAME, car_id)
        result = list()
        if indexes == None or len(indexes) == 0:
            return result
        
        for index in indexes: 
            # the structure of index: (car_id, message_id)
            header_data = HeaderData()
            # header_data.message_id = index[1]
            entry = self.fdb_tool.query(self.fdb_tool.db, TABLE_NAME, index[1])
            if entry != None:
                # header_data.time_stamp = entry[0]
                # header_data.car_id = entry[1]
                header_data = self.check_column(index[1], entry, column)
                result.append(header_data)

        return result

      
    