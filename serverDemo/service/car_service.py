import sys
sys.path.append("..")

from ctypes import sizeof
import struct 
from entity import GeoData
from entity import HeaderData
from service import DataService
from repository import HeaderRepo
from repository import GeoRepo


TIMESTAMP_LENGTH = 19
FLOAT_LENGTH = 8
INT_LENGTH = 4

class CarService(object):
    def __init__(self):
        self.header_repo = HeaderRepo()
        self.geo_repo = GeoRepo()


    '''
    :parse inbound data and get the attributes
    :format: time_stamp | carid (int) | position:x, y (float64) | linear_vel: v_x, v_y (float64) | ang_vel: v_r (float64) | direction (float64)
    :return: the attributes
    '''
    def parse_inbound(self, bytes):
        pos = 0
        timestamp = bytes[pos: pos + TIMESTAMP_LENGTH].decode('utf-8')
        print('timestamp: ', timestamp)
        pos += TIMESTAMP_LENGTH 
        car_id = int(bytes[pos: pos + INT_LENGTH].decode('utf-8'))
        print('car_id: ', car_id)
        pos += INT_LENGTH 
        x = float(bytes[pos: pos + FLOAT_LENGTH].decode('utf-8'))   # TODO: the accuracy is unchecked
        print('x: ', x)
        pos += FLOAT_LENGTH 
        y = float(bytes[pos: pos + FLOAT_LENGTH].decode('utf-8'))
        print('y: ', y)
        pos += FLOAT_LENGTH
        v_x = float(bytes[pos: pos + FLOAT_LENGTH].decode('utf-8'))
        print('v_x: ', v_x)
        pos += FLOAT_LENGTH 
        v_y = float(bytes[pos: pos + FLOAT_LENGTH].decode('utf-8'))
        print('v_y: ', v_y)
        pos += FLOAT_LENGTH 
        v_r = float(bytes[pos: pos + FLOAT_LENGTH].decode('utf-8'))
        print('v_r: ', v_r)
        pos += FLOAT_LENGTH
        direction = float(bytes[pos: pos + FLOAT_LENGTH].decode('utf-8'))
        print('direction: ', direction)
        pos += FLOAT_LENGTH 
        image = bytes[pos:].decode()
        return timestamp, car_id, x, y, v_x, v_y, v_r, direction, image


    '''
    :receive car's data and save in the database
    :return true(success) or false(fail)
    '''
    def handle_receive(self, bytes):
        timestamp, car_id, x, y, v_x, v_y, v_r, direction, image = self.parse_inbound(bytes)
        message_id = DataService.get_counter()
        print('new_message id: ', message_id)
        ret = self.header_repo.save(message_id, timestamp, car_id)
        assert ret == True
        ret = self.geo_repo.save(message_id, x, y, v_x, v_y, v_r, direction)
        assert ret == True
        return ret
    
