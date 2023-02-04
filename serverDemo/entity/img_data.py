import sys
sys.path.append("..")

IMG_COLUMNS = ['message_id', 'img']

class ImgData(object):
    # def __init__(self, img):
    def __init__(self, *x):
        if len(x) == 2:
            self.message_id = x[0]
            self.img = x[1]

    def __lt__(self, other):
        if self.message_id < other.message_id:
            return True
        else:
            return False
        
    def __str__(self):
        return ('message_id: ' + str(self.message_id) + '\t' + 'img: ' + str(self.img) + '\n')

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