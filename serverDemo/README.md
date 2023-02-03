# README
* 在fdb存取的时候，向上暴露的接口，value的类型必须是tuple
* 不支持subquery
* 不支持limit_and_offset
* 目前只支持在where之后的条件筛选
* 目前暂时不支持'join'
* 目前直接用了parser生成的表达式树，不支持优先级

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

## 目前支持的sql:
* 查询一定时间范围内的数据：