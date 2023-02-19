package org.sjtu.se.ipads.fdbserver.sqlparser.adapter;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;

import java.text.ParseException;
import java.util.Date;
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


    private abstract static class RowConverter {
        abstract Object convertRow(String[] rows);

        protected Object convert(FdbDataType fieldType, String string) {
            if (fieldType == null) {
                return string;
            }
            switch (fieldType) {
                case INT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Integer.parseInt(string);
//                case TIMESTAMP:
//                    if (string.length() == 0) {
//                        return null;
//                    }
//                    try {
//                        Date date = TIME_FORMAT_TIMESTAMP.parse(string);
//                        return new java.sql.Timestamp(date.getTime());
//                    } catch (ParseException e) {
//                        return null;
//                    }
                case STRING:
                default:
                    return string;
            }
        }
    }

    private static class ArrayRowConverter extends FdbEnumerator.RowConverter {
        private final FdbDataType[] fieldTypes;
        private final int[] fields;

        private ArrayRowConverter(FdbDataType[] fieldTypes, int[] fields) {
            this.fieldTypes = fieldTypes;
            this.fields = fields;
        }

        public Object convertRow(String[] strings) {
            final Object[] objects = new Object[fields.length];
            for (int i = 0; i < fields.length; i++) {
                int field = fields[i];
                objects[i] = convert(fieldTypes[field], strings[field]);
            }
            return objects;
        }
    }

    private static class SingleColumnRowConverter extends FdbEnumerator.RowConverter {
        private final FdbDataType fieldType;
        private final int fieldIndex;

        private SingleColumnRowConverter(FdbDataType fieldType, int fieldIndex) {
            this.fieldType = fieldType;
            this.fieldIndex = fieldIndex;
        }

        public Object convertRow(String[] strings) {
            return convert(fieldType, strings[fieldIndex]);
        }
    }
}
