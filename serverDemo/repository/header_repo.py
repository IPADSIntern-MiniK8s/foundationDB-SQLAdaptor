import sys
sys.path.append("..")

from entity import HeaderData
from basic_ops import FdbTool


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
    :find header data entry in database by message id
    :return: HeaderData
    '''
    def find_by_message_id(self, message_id):
        data = self.fdb_tool.query(self.fdb_tool.db, TABLE_NAME, message_id)
        header_data = HeaderData()
        if data == None or len(data) == 0:
            return header_data

        header_data.message_id = message_id
        header_data.time_stamp = data[0]
        header_data.car_id = data[1]
        return header_data


    '''
    :find header data entry in database by timestamp
    :return True(find) or False(not find)
    '''
    def find_by_timestamp(self, timestamp):
        data = self.fdb_tool.query(self.fdb_tool.db, TIME_INDEX_NAME, timestamp)
        header_data = HeaderData()
        if data == None or len(data) == 0:
            return header_data
        
        header_data.message_id = data[0]
        entry = self.fdb_tool.query(self.fdb_tool.db, TABLE_NAME, header_data.message_id)
        if entry == None:
            return False
        header_data.time_stamp = entry[0]
        header_data.car_id = entry[1]
        return header_data

    
    '''
    :find header data entry in database by carId
    :return: a list of header_data or empty list
    '''
    def find_by_car_id(self, car_id):
        indexes = self.fdb_tool.query_range(self.fdb_tool.db, CAR_INDEX_NAME, car_id)
        result = list()
        if indexes == None or len(indexes) == 0:
            return result
        
        for index in indexes: 
            # the structure of index: (car_id, message_id)
            header_data = HeaderData()
            header_data.message_id = index[1]
            entry = self.fdb_tool.query(self.fdb_tool.db, TABLE_NAME, header_data.message_id)
            if entry != None:
                header_data.time_stamp = entry[0]
                header_data.car_id = entry[1]
                result.append(header_data)

        return result
            
    