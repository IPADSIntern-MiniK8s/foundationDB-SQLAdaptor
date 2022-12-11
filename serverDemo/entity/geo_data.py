import sys
sys.path.append("..")
from basic_ops import FdbTool

GEO_COLUMNS = ['message_id', 'x', 'y', 'v_x', 'v_y', 'v_r', 'direction']

class GeoData(object):
    # def __init__(self, message_id, x, y, v_x, v_y, v_r, direction):
    def __init__(self, *x):
        if len(x) == 7:
            self.message_id = x[0]
            self.x = x[1]
            self.y = x[2]
            self.v_x = x[3]
            self.v_y = x[4]
            self.v_r = x[5]
            self.direction = x[6]


    '''
    :encode data to prepare for storage in the database
    :return: key and value(tuple)
    '''
    def encode_data(self):
        key = self.message_id
        value = (self.x, self.y, self.v_x, self.v_y, self.v_r, self.direction)
        return key, value

    '''
    :format output of this class
    :return: string
    '''
    def output(self):
        result = 'geometry data: {\n message.id: ' + str(self.message_id) + ',\n x: ' + str(self.x) +',\n y: ' \
            + str(self.y) + ',\n v_x: ' + str(self.v_x) + ',\n v_y: ' + str(self.v_y) + ',\n v_r: '\
                + str(self.v_r) + ',\n direction: ' + str(self.direction) + '\n}'
        return result