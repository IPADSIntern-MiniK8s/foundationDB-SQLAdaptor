import sys
sys.path.append("..")

from service import CarService
from repository import GeoRepo, HeaderRepo
from entity import HeaderData, GeoData

def basic_test():
    car_service = CarService()
    # generate some data
    time_stamp = '2277166961600969300'
    car_id = '0001'
    x = '0.344450'
    y = '0.563330'
    v_x = '1.333300'
    v_y = '1.998600'
    v_r = '1.776500'
    direction = '87.11110'
    image = '12449226384'
    bytes_arr = bytes(time_stamp.encode('utf-8')) + bytes(car_id.encode('utf-8')) + bytes(x.encode('utf-8')) + bytes(y.encode('utf-8')) + bytes(v_x.encode('utf-8')) + bytes(v_y.encode('utf-8')) \
                + bytes(v_r.encode('utf-8')) + bytes(direction.encode('utf-8')) + bytes(image.encode('utf-8'))
    ret = car_service.handle_receive(bytes_arr)
    assert ret == True

    print('find by message id')
    header_repo = HeaderRepo()
    geo_repo = GeoRepo()
    header_data = header_repo.find_by_message_id(3)
    header_content = header_data.output()
    geo_data = geo_repo.find_by_message_id(3)
    geo_content = geo_data.output()

    print(header_content)
    print(geo_content)

    print('find by timestamp')
    header_data = header_repo.find_by_timestamp(time_stamp)
    header_content = header_data.output()
    print(header_content)
    
    print('find by car id')
    print(int(car_id))
    header_datas = header_repo.find_by_car_id(int(car_id))
    for header_data in header_datas:
        header_content = header_data.output()
        print(header_content)


if __name__ == '__main__':
    basic_test()
