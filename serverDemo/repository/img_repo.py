import sys
from entity.img_data import IMG_COLUMNS
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
        key, value = message_id, (img,)
        ret = self.fdb_tool.add(self.fdb_tool.db, TABLE_NAME, key, value)
        return ret


    '''
    :auxiliary function: check which column to choose
    :return: GeoData
    '''
    def check_column(self, key, value, column):
        img_data = ImgData()
        img_data.message_id = key
        if column == None or IMG_COLUMNS[1] in column:
            img_data.img = value[0]
        else:
            img_data.img = None
        return img_data


    '''
    :find img data entry in database by message id
    :return: ImgData
    '''
    def find_by_message_id(self, message_id, column):
        data = self.fdb_tool.query(self.fdb_tool.db, TABLE_NAME, message_id)
        img_data = ImgData()
        if data == None or len(data) == 0:
            return None
        
        # img_data.message_id = message_id
        # img_data.img = data[0]
        img_data = self.check_column(message_id, data, column)
        return img_data
    
    '''
    : find a range of geometry data according to message id
    : return: list of GeoData
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
            # img_data = ImgData()
            # img_data.message_id = key[0]
            # img_data.img = value[0]
            img_data = self.check_column(key[0], value, column)
            result.append(img_data)

        return result

        