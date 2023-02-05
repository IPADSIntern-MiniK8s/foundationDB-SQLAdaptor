from email import header
import sys
sys.path.append("..")

from entity import HeaderData
from basic_ops import FdbTool
from entity.header_data import HEADER_COLUMNS
from repository.geometry_repo import GeoRepo
from repository.img_repo import ImgRepo
from repository.geometry_repo import TABLE_NAME as GEO_NAME
from repository.img_repo import TABLE_NAME as IMG_NAME

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
    :return: HeaderData or None
    '''
    def find_by_message_id(self, message_id, column):
        data = self.fdb_tool.query(self.fdb_tool.db, TABLE_NAME, message_id)
        header_data = HeaderData()
        if data == None or len(data) == 0:
            return None

        header_data = self.check_column(message_id, data, column)
        return header_data


    '''
    : find a range of header data according to message_id
    : return: list of HeaderData
    '''
    def find_by_message_id_range(self, column, lower_message_id=None, upper_message_id=None):
        result = list()
        if lower_message_id != None:
            lower_message_id = (lower_message_id,)
        if upper_message_id != None:
            upper_message_id = (upper_message_id,)

        data = self.fdb_tool.query_range(self.fdb_tool.db, TABLE_NAME, lower_message_id, upper_message_id)

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
            return result
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
                header_data = self.check_column(index[1], entry, column)
                result.append(header_data)

        return result


    '''
    :find header_data entry, index entry, geo_data entry and img_data entry in database by message id for rollback
    :return:a list: [header_data entry, index entry, geo_data entry, img_data entry] or None(Fail or incomplete)
    '''
    def find_for_rollback(self, message_id):
        geo_repo = GeoRepo()
        img_repo = ImgRepo()
        header_entry = self.find_by_message_id(message_id, None)
        if header_entry == None:
            return None
        time_key, time_value, car_key, car_value = header_entry.encode_index()
        time_index_entry = self.fdb_tool.query(self.fdb_tool.db, TIME_INDEX_NAME, time_key)
        if time_index_entry == None:
            return None
        car_index_entry = self.fdb_tool.query(self.fdb_tool.db, CAR_INDEX_NAME, car_key)
        if car_index_entry == None:
            return None
        geo_entry = geo_repo.find_by_message_id(message_id, None)
        if geo_entry == None:
            return None
        img_entry = img_repo.find_by_message_id(message_id, None)
        if img_entry == None:
            return None
        return [header_entry, geo_entry, img_entry]

    
    '''
    :rollback if delete operation fail
    :return: void
    '''
    def rollback(self, data):
        assert len(data) == 3
        geo_repo = GeoRepo()
        img_repo = ImgRepo()
        self.save(data[0].message_id, data[0].time_stamp, data[0].car_id)
        geo_repo.save(data[1].message_id, data[1].x, data[1].y. data[1].v_x, data[1].v_y, data[1].v_r, data[1].direction)
        img_repo.save(data[2].message_id, data[2].img)


    '''
    :delete header_data entry, index entry, geo_data entry and img_data entry in database by message id
    :if fail, rollback
    :return: Success(True) or Fail(False)
    '''
    def del_by_message_id(self, message_id):
        # first find all data-to-delete for rollback
        data_for_del = self.find_for_rollback(message_id)
        if data_for_del == None:
            return False
        
        # del the data
        time_stamp = data_for_del[0].time_stamp
        car_id = data_for_del[0].car_id
        ret = self.fdb_tool.remove(self.fdb_tool.db, TABLE_NAME, message_id)
        if not ret:
            self.rollback(data_for_del)
            return False
        
        ret = self.fdb_tool.remove(self.fdb_tool.db, TIME_INDEX_NAME, (time_stamp, message_id))
        if not ret:
            self.rollback(data_for_del)
        
        ret = self.fdb_tool.remove(self.fdb_tool.db, CAR_INDEX_NAME, (car_id, message_id))
        if not ret:
            self.rollback(data_for_del)
        
        ret = self.fdb_tool.remove(self.fdb_tool.db, GEO_NAME, message_id)
        if not ret:
            self.rollback(data_for_del)
        
        ret = self.fdb_tool.remove(self.fdb_tool.db, IMG_NAME, message_id)
        if not ret:
            self.rollback(data_for_del)
        
        return True


    '''
    :find header_data entry, index entry, geo_data entry and img_data entry in database by message id for rollback
    :return:a list :[[header_data entries], [geo_data entries], [img_data entries]] or None(Fail or incomplete)
    '''
    def find_range_for_rollback(self, lower_bound, upper_bound):
        geo_repo = GeoRepo()
        img_repo = ImgRepo()
        header_entries = self.find_by_message_id_range(None, lower_bound, upper_bound)
        if len(header_entries) == 0:
            return None
        geo_entries = geo_repo.find_by_message_id_range(None, lower_bound, upper_bound)
        if len(header_entries) != len(geo_entries):
            return None
        img_entries = img_repo.find_by_message_id_range(None, lower_bound, upper_bound)
        if len(img_entries) != len(header_entries):
            return None
        return [header_entries, geo_entries, img_entries]


    '''
    :rollback batch if deleting a set of data fail
    :return: void
    '''
    def batch_rollback(self, data):
        for header_data, geo_data, img_data in zip(data[0], data[1], data[2]):
            deleted = [header_data, geo_data, img_data]
            self.rollback(deleted)


    '''
    :delete header_data entry, index entry, geo_data entry and img_data entry in database by a range of message id
    :if fail, rollback
    :return: Success(True) or Fail(False) 
    '''
    def del_by_message_id_range(self, lower_bound, upper_bound):
        # find data for rollback
        data_for_del = self.find_range_for_rollback(lower_bound, upper_bound)
        if data_for_del == None:
            return False
        
        if lower_bound != None:
            lower_bound = (lower_bound,)
        if upper_bound != None:
            upper_bound = (upper_bound,)

        ret = self.fdb_tool.remove_range(self.fdb_tool.db, TABLE_NAME, lower_bound, upper_bound)
        if not ret:
            self.batch_rollback(data_for_del) 
            return False
        
        ret = self.fdb_tool.remove_range(self.fdb_tool.db, GEO_NAME, lower_bound, upper_bound)
        if not ret:
            self.batch_rollback(data_for_del) 
            return False

        ret = self.fdb_tool.remove_range(self.fdb_tool.db, IMG_NAME, lower_bound, upper_bound)
        if not ret:
            self.batch_rollback(data_for_del) 
            return False

        for header_entry in data_for_del[0]:
            message_id = header_entry.message_id
            car_id = header_entry.car_id
            time_stamp = header_entry.time_stamp
            ret = self.fdb_tool.remove(self.fdb_tool.db, TIME_INDEX_NAME, (time_stamp, message_id))
            if not ret:
                self.rollback(data_for_del)
            
            ret = self.fdb_tool.remove(self.fdb_tool.db, CAR_INDEX_NAME, (car_id, message_id))
            if not ret:
                self.rollback(data_for_del)
            
        return True