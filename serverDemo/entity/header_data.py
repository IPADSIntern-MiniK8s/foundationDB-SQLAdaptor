import sys
sys.path.append("..")

HEADER_COLUMNS = ['message_id', 'time_stamp', 'car_id']

class HeaderData(object):
    # def __init__(self, message_id, time_stamp, car_id):
    def __init__(self, *x):
        if len(x) == 3:
            self.message_id = x[0]
            self.time_stamp = x[1]
            self.car_id = x[2]
    
    def __lt__(self, other):
        if self.message_id < other.message_id:
            return True
        else:
            return False
    
    def __str__(self):
        return ('message_id: ' + 
        str(self.message_id) + '\t' + 'time_stamp: ' + str(self.time_stamp) + '\t' + 
        'car_id: ' + str(self.car_id) + '\n')

    '''
    :encode data to prepare for storing in the database
    :return: key and value(tuple)
    '''
    def encode_data(self):
        key = self.message_id
        value = (self.time_stamp, self.car_id)
        return key, value


    '''
    :encode index for storing in the database
    :return: key and value(tuple)
    '''
    def encode_index(self):
        # encode timestamp
        time_key = (self.time_stamp, self.message_id)
        time_value = tuple()
        car_key = (self.car_id, self.message_id)
        car_value = tuple()
        return time_key, time_value, car_key, car_value

    '''
    :format output of this class
    :return: string
    '''
    def output(self):
        result = 'header_data: {\n message id: ' + str(self.message_id) + ',\n time_stamp: '    \
            + str(self.time_stamp) + ',\n car id: ' + str(self.car_id) + '\n}'
        return result
        
            


    
    

    



