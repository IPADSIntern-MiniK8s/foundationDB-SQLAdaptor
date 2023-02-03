import sys
sys.path.append("..")

from entity import ImgData
from basic_ops import FdbTool

TABLE_NAME = 'picture'

class ImgRepo(object):
    def __init__(self):
        self.fdb_tool = FdbTool()

    '''
    :save the img_data
    '''
    def save(self, message_id, img):
        img_data = ImgData(message_id, img)
        key, value = img_data.encode_data()
        ret = self.fdb_tool.add(self.fdb_tool.db, TABLE_NAME, key, value)
        return ret

    '''
    :find img data entry in database by message id
    :return: ImgData
    '''
    def find_by_message_id(self, message_id):
        data = self.fdb_tool.query(self.fdb_tool.db, TABLE_NAME, message_id)
        img_data = ImgData()
        if data == None or len(data) == 0:
            return img_data
        
        img_data.message_id = message_id
        img_data.img = data[0]
        return img_data
    
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
            img_data = ImgData()
            img_data.message_id = key[0]
            img_data.img = value[0]
            result.append(img_data)

        return result

        