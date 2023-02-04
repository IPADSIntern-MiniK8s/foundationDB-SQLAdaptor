import sys
sys.path.append("..")

from service import ClientService, DataService


def select_test():
    client_service = ClientService()
    sqls = ["Select * from header where message_id = 1;", 
            "Select * from geometry where message_id = 2;",
            "Select * from picture where message_id = 3;",
            "Select message_id, time_stamp, car_id from header;", 
            "Select * from header where message_id > 1 and message_id < 6;",
            "Select * from header where message_id >= 1 and message_id < 6;",
            "Select * from header where message_id > 1 and message_id <= 6;",
            "Select * from header where message_id >= 1 and message_id <= 6;",
            "Select * from header where message_id > 1 and message_id <= 6 and car_id = 1;",
            "Select * from header where message_id = 1 or message_id = 6;",
            "Select * from header where time_stamp > 2277166961600969300 and time_stamp <= 2277166961600969500;" 
            ]
    for sql in sqls:
        result = client_service.query_by_sql(sql)
        print('sql: ', sql)
        for elem in result:
            print(elem, end='')


# NOTE: if inserted value is string, it must be quoted
def insert_test():
    data_service = DataService()
    client_service = ClientService()
    message_id = data_service.get_counter()
    sqls = ["INSERT INTO header (message_id, time_stamp, car_id) VALUES (" + str(message_id) + ", '2277166961600969700', 1);",
            "INSERT INTO geometry (message_id, x, y, v_x, v_y, v_r, direction) VALUES (" + str(message_id) + ", 11344450, 11344450, 11344450, 11344450, 11344450, 11344450);",
            "INSERT INTO picture (message_id, img) VALUES (" + str(message_id) + ", '2277166961600969700');"]

    for sql in sqls:
        print('sql: ', sql)
        result = client_service.query_by_sql(sql)
        for elem in result:
            print(elem)

if __name__ == "__main__":
    # select_test()
    insert_test()