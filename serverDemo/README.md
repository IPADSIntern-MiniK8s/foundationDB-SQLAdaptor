# README
* 在fdb存取的时候，向上暴露的接口，value的类型必须是tuple
* 不支持subquery
* 不支持limit_and_offset
* 目前只支持在where之后的条件筛选
* 目前暂时不支持'join'
* 目前直接用了parser生成的表达式树，不支持优先级(可以考虑直接用eval代替)
* 速度和位置数据都是int形式存储的 

### logical plan的一些例子
* "INSERT INTO Websites (name, url, alexa, country) VALUES ('百度','https://www.baidu.com/','4','CN');"
  ```
    query_type:  QueryType.INSERT
    tables:  ['Websites']
    columns_dict:  {'insert': ['name', 'url', 'alexa', 'country']}
    value_dict:  {'name': '百度', 'url': 'https://www.baidu.com/', 'alexa': '4', 'country': 'CN'}
    exp:  []
    union ops:  []
  ```
* "UPDATE Websites SET alexa='5000', country='USA' WHERE name=lecture;"
  ```
    query_type:  QueryType.UPDATE
    tables:  ['Websites']
    columns_dict:  {'update': ['alexa', 'country'], 'where': ['name', 'lecture']}
    value_dict:  {'alexa': '5000', 'country': 'USA'}
    exp:  [['name', '=', 'lecture']]
    union ops:  []
  ```
* "DELETE FROM Websites WHERE name=Facebook AND country=USA;"
  ```
    query_type:  QueryType.DELETE
    tables:  ['Websites']
    columns_dict:  {'where': ['name', 'Facebook', 'country', 'USA']}
    value_dict:  None
    exp:  [['name', '=', 'Facebook'], ['country', '=', 'USA']]
    union ops:  [<Ops.AND_OP: '&'>]
  ```
* "Select * from emp where sal > 2000 AND sal < (3000 - (3.0 * 5));"
```
    execution plan:  {'type': <QueryType.SELECT: 'SELECT'>, 'table_name': ['emp'], 'columns': ['*'], 'conditions': [['sal', '>', '2000'], ['sal', '<', '2985.0']], 'union_ops': [<Ops.AND_OP: '&'>]}
```

* execute plan的输出格式：
```
------------------------- test select -------------------------
Select * from emp where sal > 2000 AND sal < (3000 - (3.0 * 5));
execution plan:  {'type': <QueryType.SELECT: 'SELECT'>, 'table_name': ['emp'], 'columns': ['*'], 'conditions': [['sal', '>', '2000'], ['sal', '<', '2985.0']], 'union_ops': [<Ops.AND_OP: '&'>]}
Select name1, name2 from emp where sal > 2000 AND sal < (3000 - (3.0 * 5));
execution plan:  {'type': <QueryType.SELECT: 'SELECT'>, 'table_name': ['emp'], 'columns': ['name1', 'name2'], 'conditions': [['sal', '>', '2000'], ['sal', '<', '2985.0']], 'union_ops': [<Ops.AND_OP: '&'>]}
------------------------- test insert -------------------------
INSERT INTO Websites (name, url, alexa, country) VALUES ('百度','https://www.baidu.com/','4','CN');
execution plan:  {'type': <QueryType.INSERT: 'INSERT'>, 'table_name': ['Websites'], 'values': {'name': '百度', 'url': 'https://www.baidu.com/', 'alexa': '4', 'country': 'CN'}}
------------------------- test update -------------------------
UPDATE Websites SET alexa='5000', country='USA' WHERE name=lecture;
execution plan:  {'type': <QueryType.UPDATE: 'UPDATE'>, 'table_name': ['Websites'], 'columns': ['alexa', 'country'], 'values': {'alexa': '5000', 'country': 'USA'}, 'conditions': [['name', '=', 'lecture']], 'union_ops': []}
------------------------- test delete -------------------------
DELETE FROM Websites WHERE name=Facebook AND country=USA;
execution plan:  {'type': <QueryType.DELETE: 'DELETE'>, 'table_name': ['Websites'], 'conditions': [['name', '=', 'Facebook'], ['country', '=', 'USA']], 'union_ops': [<Ops.AND_OP: '&'>]}
```

## 目前支持的sql:
* 查询一定时间范围内的数据：

### 可以支持的sql举例（已经通过测试了）：
* 具体参考`pysical_plan_test`
* delete相关的sql只支持对于`header`的操作，同时删除`geometry`和`picture`中的内容，因为`header`,`geometry`,`picture`同时作为一条记录，单独删除其中一个是没有意义的

### 目前的bug
* 范围查找还有点小错
* 查找结果为空时有`AttributeError`