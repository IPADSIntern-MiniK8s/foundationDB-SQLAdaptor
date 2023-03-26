import sys
sys.path.append("./server")

from service import CarService
from repository import GeoRepo, HeaderRepo
from entity import HeaderData, GeoData

def store_data(time_stamp,car_id,x,y,direction,v_x,v_y,v_r,image):
    car_service = CarService()
    bytes_arr = bytes(time_stamp.encode('utf-8')) + bytes(car_id.encode('utf-8')) + bytes(x.encode('utf-8')) + bytes(y.encode('utf-8')) + bytes(v_x.encode('utf-8')) + bytes(v_y.encode('utf-8')) \
                + bytes(v_r.encode('utf-8')) + bytes(direction.encode('utf-8')) + bytes(image.encode('utf-8'))
    ret = car_service.handle_receive(bytes_arr)
    assert ret == True


    print('find by message id')
    header_repo = HeaderRepo()
    geo_repo = GeoRepo()
    header_data = header_repo.find_by_message_id(1, None)
    header_content = header_data.output()
    print(header_data)
    geo_data = geo_repo.find_by_message_id(1, None)
    geo_content = geo_data.output()
    print(geo_data)
    return geo_content

    print(header_content)
    print(geo_content)

    print('find by timestamp')
    header_data = header_repo.find_by_timestamp(time_stamp, None)
    header_content = header_data#.output()
    print(header_content)

    print('find by car id')
    print(int(car_id))
    header_datas = header_repo.find_by_car_id(int(car_id), None)
    for header_data in header_datas:
        header_content = header_data#.output()
        print(header_content)

