import sys
sys.path.append("..")
from basic_ops import FdbTool

"""Unit tests for basic-ops"""



def basic_test():
    fdb_tool = FdbTool(['test1'])
    # test write
    ret = fdb_tool.add(fdb_tool.db, 'test1', 'test_key1', ('test_value1',))
    assert ret == True
    # test read
    value = fdb_tool.query(fdb_tool.db, 'test1', ('test_key1',))
    print('the value get from database: ', value)
    # test update
    ret = fdb_tool.update(fdb_tool.db, 'test1', 'test_key1', ('modified1',))
    assert ret == True
    # test read after modified
    value = fdb_tool.query(fdb_tool.db, 'test1', 'test_key1')
    print('the value get from database: ', value)
    # test delete
    ret = fdb_tool.remove(fdb_tool.db, 'test1', 'test_key1')
    assert ret == True
    # test read after delete
    value = fdb_tool.query(fdb_tool.db, 'test1', 'test_key1')
    print('the value get from database: ', value)
    # test query all
    ret = fdb_tool.add(fdb_tool.db, 'test1', 'test_key1', ('test_value1',))
    assert ret == True
    ret = fdb_tool.add(fdb_tool.db, 'test1', 'test_key2', ('test_value2',))
    assert ret == True
    value = fdb_tool.query_all(fdb_tool.db, 'test1')
    print('the whole value get from database: ', value)

    # test query range

    # ret = fdb_tool.add(fdb_tool.db, 'test1', 'test_key2', ('test_value1',))
    # assert ret == True
    ret = fdb_tool.add(fdb_tool.db, 'test1', 'test_key3', ('test_value1',))
    assert ret == True
    value = fdb_tool.query_range(fdb_tool.db, 'test1', ('test_key1',), ('test_key3',))
    print('the range value get from database: ', value) # output:  {('test_key1',): ('test_value1',), ('test_key2',): ('test_value2',)}
    value = fdb_tool.query_range(fdb_tool.db, 'test1', ('test_key1',), None)
    print('the range value get from database: ', value) # output:  {('test_key1',): ('test_value1',), ('test_key2',): ('test_value2',), ('test_key3',): ('test_value1',)}
    value = fdb_tool.query_range(fdb_tool.db, 'test1', None, ('test_key4',))
    print('the range value get from database: ', value) # {('test_key1',): ('test_value1',), ('test_key2',): ('test_value2',), ('test_key3',): ('test_value1',)}

if __name__ == '__main__':
    basic_test()
