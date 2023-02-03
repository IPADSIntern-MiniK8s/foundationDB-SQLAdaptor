import sys
sys.path.append("..")

from sqlparser import ExecutionBuilder

def basic_test():
    execution_builder = ExecutionBuilder()
    print('------------------------- test select -------------------------')
    sql = "Select * from emp where sal > 2000 AND sal < (3000 - (3.0 * 5));"
    print(sql)
    execution_builder.generate_execution(sql)

    sql = "Select name1, name2 from emp where sal > 2000 AND sal < (3000 - (3.0 * 5));"
    print(sql)
    execution_builder.generate_execution(sql)

    print('------------------------- test insert -------------------------')
    sql = "INSERT INTO Websites (name, url, alexa, country) VALUES ('百度','https://www.baidu.com/','4','CN');"
    print(sql)
    execution_builder.generate_execution(sql)

    print('------------------------- test update -------------------------')
    sql = "UPDATE Websites SET alexa='5000', country='USA' WHERE name=lecture;"
    print(sql)
    execution_builder.generate_execution(sql)

    print('------------------------- test delete -------------------------')
    sql = "DELETE FROM Websites WHERE name=Facebook AND country=USA;"
    print(sql)
    execution_builder.generate_execution(sql)

if __name__ == '__main__':
    basic_test()
