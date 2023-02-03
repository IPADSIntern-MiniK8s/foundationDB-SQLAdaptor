import sys

from serverDemo.entity import geo_data
sys.path.append("..")

from entity import GeoData
from basic_ops import FdbTool
from entity.geo_data import GEO_COLUMNS

TABLE_NAME = 'geometry'

class GeoRepo(object):
    def __init__(self):
        self.fdb_tool = FdbTool()

    '''
    :save the geometry_data
    '''
    def save(self, message_id, x, y, v_x, v_y, v_r, direction):
        geo_data = GeoData(message_id, x, y, v_x, v_y, v_r, direction)
        key, value = geo_data.encode_data()
        ret = self.fdb_tool.add(self.fdb_tool.db, TABLE_NAME, key, value)
        return ret


    '''
    :auxiliary function: check which column to choose
    :return: GeoData
    '''
    def check_column(self, key, value, column):
        geo_data = GeoData()
        if column == None or GEO_COLUMNS[0] in column:
            geo_data.message_id = key
        else:
            geo_data.message_id = None
        if column == None or GEO_COLUMNS[1] in column:
            geo_data.x = value[0]
        else:
            geo_data.x = None
        if column == None or GEO_COLUMNS[2] in column:
            geo_data.y = value[1]
        else:
            geo_data.y = None
        if column == None or GEO_COLUMNS[3] in column:
            geo_data.v_x = value[2]
        else:
            geo_data.v_x = None
        if column == None or GEO_COLUMNS[4] in column:
            geo_data.v_y = value[3]
        else:
            geo_data.v_y = None
        if column == None or GEO_COLUMNS[5] in column:
            geo_data.v_r = value[4]
        else:
            geo_data.v_r = None
        if column == None or GEO_COLUMNS[6] in column:
            geo_data.direction = value[5]
        else:
            geo_data.direction = None
        return geo_data


    '''
    :find geometry data entry in database by message id
    :return: GeoData
    '''
    def find_by_message_id(self, message_id):
        data = self.fdb_tool.query(self.fdb_tool.db, TABLE_NAME, message_id)
        geo_data = GeoData()
        if data == None or len(data) == 0:
            return geo_data
        
        geo_data.message_id = message_id
        geo_data.x = data[0]
        geo_data.y = data[1]
        geo_data.v_x = data[2]
        geo_data.v_y = data[3]
        geo_data.v_r = data[4]
        geo_data.direction = data[5]
        return geo_data

    '''
    : find a range of geometry data according to message id
    : return: list of GeoData
    '''
    def find_by_message_id_range(self, lower_message_id=None, upper_message_id=None):
        result = list()
        data = self.fdb_tool.query_range(self.fdb_tool.db, TABLE_NAME, lower_message_id, upper_message_id)
        
        if data == None or len(data) == 0:
            return result

        for key, value in data.items():
            geo_data = GeoData()
            geo_data.message_id = key[0]    
            geo_data.x = value[0]
            geo_data.y = value[1]
            geo_data.v_x = value[2]
            geo_data.v_y = value[3]
            geo_data.v_r = value[4]
            geo_data.direction = value[5]
            result.append(geo_data)

        return result

    '''
    : find geometry data according to an attribute in a range (it doesn't have index now, can supply when need)
    :NOTE: the interval left-closed and right-open
    : return: list of GeoData
    '''
    def find_by_unindex_attribute_range(self, attr, lower_val, upper_val):
        result = list()
        index = 0

        if attr == 'y':
            index = 1
        elif attr == 'v_x':
            index = 2
        elif attr == 'v_y':
            index = 3
        elif attr == 'v_r':
            index = 4
        elif attr == 'direction':
            index = 5
        elif attr != 'x':
            return result
        
        data = self.fdb_tool.query_all(self.fdb_tool.db, TABLE_NAME)
        if data == None or len(data) == 0:
            return result
        
        for key, value in data.items():
            if (lower_val == None or (lower_val != None and value[index] >= lower_val)) and (upper_val == None or(value[index] < upper_val)):
                geo_data = GeoData()
                geo_data.message_id = key
                geo_data.x = value[0]
                geo_data.y = value[1]
                geo_data.v_x = value[2]
                geo_data.v_y = value[3]
                geo_data.v_r = value[4]
                geo_data.direction = value[5]
                result.append(geo_data)
            
        return result
