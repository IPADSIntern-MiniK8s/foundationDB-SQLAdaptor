import sys
sys.path.append("..")

IMG_COLUMNS = ['message_id', 'img']

class ImgData(object):
    # def __init__(self, img):
    def __init__(self, *x):
        if len(x) == 2:
            self.message_id = x[0]
            self.img = x[1]

    '''
    :encode data to prepare for storing in the database
    :return: key and value(tuple)
    '''
    def encode_data(self):
        key = self.message_id
        value = (self.img)
        return key, value
    
    '''
    :format output of this class
    :return: string
    '''
    def output(self):
        result = 'img data: {\n message_id: ' + str(self.message_id) + ',\n img: ' + str(self.img) + '\n}'
        return result