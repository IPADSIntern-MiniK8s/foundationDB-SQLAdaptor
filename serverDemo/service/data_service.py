import logging
from entity.geo_data import GeoData
from entity.header_data import HeaderData
from basic_ops import FdbTool

TABLE_NAMES = ['header', 'geometry', 'picture']

class DataService(object):
    fdb_tool = FdbTool()
    # get the biggest message Id in the past
    all_headers = fdb_tool.query_all(fdb_tool.db, TABLE_NAMES[0])
    # the message id for next entry
    # NOTE: only available for single thread
    # check empty
    if len(all_headers.keys()) < 1:
        id_counter = 1
    else:
        id_counter = sorted(all_headers.keys())[-1] + 1
        

    '''
    :encode the data and store in the database
    :TODO: add index
    :return: success(true) or fail(false)
    '''
    @classmethod
    def get_counter(cls):
        former = cls.id_counter
        cls.id_counter += 1
        return former

    
        

        
