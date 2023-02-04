import sys
sys.path.append("..")

from service import CarService
from repository import GeoRepo, HeaderRepo
from entity import HeaderData, GeoData

def generate():
    car_service = CarService()
    time_stamps = ['2277166961600969300', '2277166961600969400', '2277166961600969500']
    car_ids = ['0001', '0002', '0003', '0004']
    xs = ['10344450', '11344450', '12344450', '13344450']
    y = '01563330'
    v_x = '10333300'
    v_y = '10998600'
    v_r = '10776500'
    direction = '87011110'
    image = '12449226384'
    for time_stamp in time_stamps:
        for car_id in car_ids:
            for x in xs:
                bytes_arr = bytes(time_stamp.encode('utf-8')) + bytes(car_id.encode('utf-8')) + bytes(x.encode('utf-8')) + bytes(y.encode('utf-8')) + bytes(v_x.encode('utf-8')) + bytes(v_y.encode('utf-8')) \
                + bytes(v_r.encode('utf-8')) + bytes(direction.encode('utf-8')) + bytes(image.encode('utf-8'))
                ret = car_service.handle_receive(bytes_arr)
                assert ret == True


generate()