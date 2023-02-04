import fdb
import fdb.tuple

fdb.api_version(710)
DATABASE_NAME = ('car_data',)
TABLE_NAMES = ['header', 'geometry', 'picture', 'time_index', 'car_index', 'test1']
TABLE_INDEXS = {'header': 't0', 
                'geometry': 't1',
                'picture': 't2', 
                'time_index': 'i0', 
                'car_index': 'i1',
                'test1': 'r1',
                }


class FdbTool(object):
    def __init__(self, table_names = None):
        # TODO： when have multiple type values, consider using subspace
        self.db = fdb.open()
        self.db.options.set_transaction_timeout(6000)
        self.db.options.set_transaction_retry_limit(100)
        self.dir = fdb.directory.create_or_open(self.db, (DATABASE_NAME))

        self.tables = list()
        
        if table_names == None:
            for table_name in TABLE_NAMES:
                self.tables.append(table_name)

        # user defined table name
        else:
            for table_name in table_names:
                self.tables.append(table_name)
    

    '''
    :add element to database
    :return: success(true) or fail(false)
    '''
    @fdb.transactional
    def add(self, tr, table_name, key, value):
        if table_name not in self.tables:
            return False
        if isinstance(key, tuple):
            packed_key = self.dir[TABLE_INDEXS[table_name]].pack(key)
        else:
            packed_key = self.dir[TABLE_INDEXS[table_name]].pack((key,))
        self.db[packed_key] = fdb.tuple.pack(value)
        return True
    

    '''
    :delete element from database
    :return: success(true) or fail(false)
    '''
    @fdb.transactional
    def remove(self, tr, table_name, key):
        # first check the table name
        if table_name not in self.tables:
            return False
        packed_key = self.dir[TABLE_INDEXS[table_name]].pack((key,))
        # check whether the data exist
        if self.db[packed_key] == None:
            return True
        del self.db[packed_key]
        return True

    
    '''
    :update element in database
    :return: success(true) or fail(false)
    '''
    @fdb.transactional
    def update(self, tr, table_name, key, value):
        # first check the table name
        if table_name not in self.tables:
            return False
        
        if isinstance(key, tuple):
            packed_key = self.dir[TABLE_INDEXS[table_name]].pack(key)
        else:
            packed_key = self.dir[TABLE_INDEXS[table_name]].pack((key,))
        # check whether the data exist, if not exist, equal to insert 
        # if self.db[packed_key] == None:
        #     return False
        self.db[packed_key] = fdb.tuple.pack(value)
        return True


    '''
    :query data from database
    :return: the data wanted or empty string
    '''
    @fdb.transactional
    def query(self, tr, table_name, key):
        # first check the table name
        if table_name not in self.tables:
            return ''
        packed_key = self.dir[TABLE_INDEXS[table_name]].pack((key,))
        # the return data can be different types
        # all value type is tuple
        val = self.db[packed_key]

        if self.db[packed_key] == None:
            return ''
        result = fdb.tuple.unpack(val)
        return result


    # TODO: There is one thing that needs to be unified here: 
    # `query_all` returns  a key which is a single value, 
    # while query_range returns a tuple
    '''
    :get the specific whole table from database
    :return: the data wanted (dict) or None
    '''
    @fdb.transactional
    def query_all(self, tr, table_name):
        if table_name not in self.tables:
            return None 
        else:
            raw_data = self.db[self.dir[TABLE_INDEXS[table_name]].range(())]
            # unpack the key and value
            result = dict()
            for k, v in raw_data:
                key = self.dir[TABLE_INDEXS[table_name]].unpack(k)[0]  
                value = fdb.tuple.unpack(v)
                result[key] = value
            return result


    '''
    :get a set of data satisfied the request
    :return: the data wanted (dict) or None
    '''
    @fdb.transactional
    def query_condition_all(self, tr, table_name, key):
        if table_name not in self.tables:
            return None
        else:
            raw_data = self.db[self.dir[TABLE_INDEXS[table_name]].range((key,))]
            result = dict()
            for k, v in raw_data:
                key = self.dir[TABLE_INDEXS[table_name]].unpack(k)
                value = fdb.tuple.unpack(v)
                result[key] = value
            return result
                  
    
    '''
    :get a set of data satisfied the range
    :param: upper bound and lower bound should be tuple
    :NOTE: the interval left-closed and right-open
    :return: the data wanted (dict) or None
    '''
    @fdb.transactional
    def query_range(self, tr, table_name, lower_bound=None, upper_bound=None):
        packed_upper_key = '\xFF'
        packed_lower_key = ''

        # TODO: for the infinite bound, I am not sure
        if table_name not in self.tables:
            return None
        else:
            if upper_bound != None:
                packed_upper_key = self.dir[TABLE_INDEXS[table_name]].pack(upper_bound)
            else: 
                packed_upper_key = self.dir[TABLE_INDEXS[table_name]].pack(('\xFF',))
            if lower_bound != None:
                packed_lower_key = self.dir[TABLE_INDEXS[table_name]].pack(lower_bound)
            else:
                packed_lower_key = self.dir[TABLE_INDEXS[table_name]].pack(('',))

            # raw_data = self.db.get_range(packed_lower_key, packed_upper_key)
            raw_data = self.db.get_range(packed_lower_key, packed_upper_key)
            result = dict()
            for k, v in raw_data:
                key = self.dir[TABLE_INDEXS[table_name]].unpack(k)
                value = fdb.tuple.unpack(v)
                result[key] = value
            return result


