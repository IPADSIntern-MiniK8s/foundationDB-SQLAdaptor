import sys
sys.path.append("..")

from service import ClientService, DataService


def select_test():
    client_service = ClientService()
    sqls = ["Select * from header where message_id = 1;", 
            "Select * from geometry where message_id = 2;",
            "Select * from geometry where x = 10344450;",
            "Select * from geometry where x > 10344450 and x < 13344450;",
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


def update_test():
    client_service = ClientService()
    sqls = ["UPDATE header SET time_stamp='2377166961600969400', car_id=5 WHERE message_id = 1;",
            "UPDATE header SET time_stamp='2388166961600969400', car_id=6 WHERE message_id > 2 and message_id < 8;",
            "UPDATE geometry SET x=11344460 WHERE x=11344450;",
            "UPDATE picture SET img='1111111111' WHERE message_id = 10;"
            ]

    for sql in sqls:
        print('sql: ', sql)
        result = client_service.query_by_sql(sql)
        for elem in result:
            print(elem, end='')


def delete_test():
    client_service = ClientService()
    sqls = ["DELETE FROM header WHERE message_id = 12",
            "DELETE FROM header WHERE message_id > 15 and message_id < 18;",
            "Select * from header where message_id = 12;", 
            "Select * from header where message_id > 15 and message_id <18;", 
            ]
    for sql in sqls:
        print('sql: ', sql)
        result = client_service.query_by_sql(sql)
        for elem in result:
            print(elem)

    
if __name__ == "__main__":
    select_test()
    # insert_test()
    # update_test()
    # delete_test()