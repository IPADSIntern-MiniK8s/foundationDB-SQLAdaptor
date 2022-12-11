# README
* 在fdb存取的时候，向上暴露的接口，value的类型必须是tuple
* 不支持subquery
* 不支持limit_and_offset
* 目前只支持在where之后的条件筛选
* 目前暂时不支持'join'
* 目前直接用了parser生成的表达式树，不支持优先级
