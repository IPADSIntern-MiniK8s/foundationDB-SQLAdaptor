import sys
sys.path.append("..")

from repository import GeoRepo, HeaderRepo, ImgRepo

def header_test():
    header_repo = HeaderRepo()

    # check `find_by_message_id`
    print('check `find_by_message_id`')
    data = header_repo.find_by_message_id(2, ['message_id', 'time_stamp'])  
    print(data, end='')

    # check `find_by_message_id_range`
    print('check `find_by_message_id_range`')
    data = header_repo.find_by_message_id_range(None, 1, 5)
    for elem in data:
        print(elem, end='')

    # check `find_all`
    print('check `find_all`')
    data = header_repo.find_all(None)
    for elem in data:
        print(elem, end='')

    # check `find_by_timestamp`
    print('check `find_by_timestamp`')
    data = header_repo.find_by_timestamp('2277166961600969300', None)
    for elem in data:
        print(elem, end='')

    # check `find_by_timestamp_range`
    print('`find_by_timestamp_range`')
    data = header_repo.find_by_timestamps_range(None, '2277166961600969300', '2277166961600969400')
    for elem in data:
        print(elem, end='')

    # check `find_by_car_id`
    print('`find_by_car_id`')
    data = header_repo.find_by_car_id(1, None)
    for elem in data:
        print(elem, end='')


def geo_test():
    geo_repo = GeoRepo()

    # check `find_by_message_id`
    data = geo_repo.find_by_message_id(2, ['x', 'y'])
    print(data, end='')

    # check `find_by_message_id_range`
    print('check `find_by_message_id_range`')
    data = geo_repo.find_by_message_id_range(None, 1, 5)
    for elem in data:
        print(elem, end='')

    # check `find_by_unindex_attribute_range`
    print('check `find_by_message_id_range`')
    data = geo_repo.find_by_unindex_attribute_range('x', None, 10344450, 11344450)
    for elem in data:
        print(elem, end='')


def img_test():
    img_repo = ImgRepo()
    
    # check `find_by_message_id`
    data = img_repo.find_by_message_id(2, None)
    print(data, end='')

    # check `find_by_message_id_range`
    print('check `find_by_message_id_range`')
    data = img_repo.find_by_message_id_range(None, 1, 5)
    for elem in data:
        print(elem, end='')



def del_test():
    header_repo = HeaderRepo()
    geo_repo = GeoRepo()
    img_repo = ImgRepo()
    # check `del_by_message_id`
    print('check `del_by_message_id`')
    id = 5
    data = header_repo.del_by_message_id(id)
    print(data)
    ret = header_repo.find_by_message_id(id, None)
    assert ret == None
    ret = geo_repo.find_by_message_id(id, None)
    assert ret == None
    ret = img_repo.find_by_message_id(id, None)
    assert ret == None
    
    lower = 6
    upper = 7
    print('check `del_by_message_id_range`')
    data = header_repo.del_by_message_id_range(lower_bound=lower, upper_bound=upper)
    print(data)
    ret = header_repo.find_by_message_id_range(None, lower, upper)
    print(len(ret))
    ret = geo_repo.find_by_message_id_range(None, lower, upper)
    print(len(ret))
    ret = img_repo.find_by_message_id_range(None, lower, upper)
    print(len(ret))


if __name__ == "__main__":
    header_test()
    # geo_test()
    # img_test()
    # del_test()