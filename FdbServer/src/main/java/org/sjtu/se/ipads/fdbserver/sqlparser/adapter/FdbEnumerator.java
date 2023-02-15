package org.sjtu.se.ipads.fdbserver.sqlparser.adapter;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FdbEnumerator<E> implements Enumerator<Object[]> {
    private final Enumerator<Object[]> enumerator;

    FdbEnumerator(FdbSchema schema, String tableName) {
        FdbTableInfo tableFieldInfo = schema.getTableFieldInfo(tableName);
        FdbDataProcess dataProcess = new FdbDataProcess(tableFieldInfo);
        List<Object[]> objs = dataProcess.read();
        enumerator = Linq4j.enumerator(objs);
    }

    static Map<String, Object> deduceRowType(FdbTableInfo tableFieldInfo) {
        final Map<String, Object> fieldBuilder = new LinkedHashMap<>();
        for (LinkedHashMap<String, Object> field : tableFieldInfo.getFields()) {
            fieldBuilder.put(field.get("name").toString(), field.get("type").toString());
        }
        return fieldBuilder;
    }

    @Override public Object[] current() {
        return enumerator.current();
    }

    @Override public boolean moveNext() {
        return enumerator.moveNext();
    }

    @Override public void reset() {
        System.out.println("not support");
    }

    @Override public void close() {
        enumerator.close();
    }


}
