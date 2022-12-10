from curses.ascii import TAB
import imp
import sys
sys.path.append("..")

from entity import GeoData
from basic_ops import FdbTool

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

