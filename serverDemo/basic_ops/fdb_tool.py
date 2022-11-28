import fdb
import fdb.tuple
import os.path as osp

fdb.api_version(710)
DATABASE_NAME = ('car_data',)

class FdbTool(object):
    

    def __init__(self, table_names):
        # TODO： when have multiple type values, consider using subspace
        self.db = fdb.open()
        self.db.options.set_transaction_timeout(6000)
        self.db.options.set_transaction_retry_limit(100)
        self.dir = fdb.directory.create_or_open(self.db, (DATABASE_NAME))

        self.tables = list()
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
        self.db[self.dir[table_name].pack((key,))] = fdb.tuple.pack((value,))
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
        packed_key = self.dir[table_name].pack((key,))
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
        packed_key = self.dir[table_name].pack((key,))
        # check whether the data exist
        if self.db[packed_key] == None:
            return False
        self.db[packed_key] = fdb.tuple.pack((value,))
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
        packed_key = self.dir[table_name].pack((key,))

        # the return data can be different types
        # TODO: which type? tuple or list? need more discuss
        val = self.db[packed_key]

        if self.db[packed_key] == None:
            return ''
        result = fdb.tuple.unpack(val)
        return result
    
        
                  
    


