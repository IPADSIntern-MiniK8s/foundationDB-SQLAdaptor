package org.sjtu.se.ipads.fdbserver.adapter;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FdbTable extends AbstractTable implements ScannableTable {
    final FdbSchema schema;
    final String tableName;
    final RelProtoDataType protoRowType;
    final ImmutableMap<String, Object> allFields;
    static FdbEnumerator fdbEnumerator;

    public FdbTable(FdbSchema  schema,
                     String tableName,
                     RelProtoDataType protoRowType,
                     Map<String, Object> allFields) {
        this.schema = schema;
        this.tableName = tableName;
        this.protoRowType = protoRowType;
        this.allFields = allFields == null ? ImmutableMap.of()
                : ImmutableMap.copyOf(allFields);
    }

    // TODO: 在读取csv文件时是读取表头，不知道这里是不是还需要
    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (protoRowType != null) {
            return protoRowType.apply(typeFactory);
        }
        final List<RelDataType> types = new ArrayList<RelDataType>(allFields.size());
        final List<String> names = new ArrayList<String>(allFields.size());

        for (Object key : allFields.keySet()) {
            final RelDataType type = typeFactory.createJavaType(allFields.get(key).getClass());
            names.add(key.toString());
            types.add(type);
        }
        return typeFactory.createStructType(Pair.zip(names, types));
    }

    static Table create(
            FdbSchema schema,
            String tableName,
            Map operand,    // TODO: 这里的operand有什么用处吗
            RelProtoDataType protoRowType) {
        FdbTableInfo tableFieldInfo = schema.getTableFieldInfo(tableName);
        Map<String, Object> allFields = fdbEnumerator.deduceRowType(tableFieldInfo);
        return new FdbTable(schema, tableName, protoRowType, allFields);
    }

    static Table create(
            FdbSchema schema,
            String tableName,
            RelProtoDataType protoRowType) {
        FdbTableInfo tableFieldInfo = schema.getTableFieldInfo(tableName);
        Map<String, Object> allFields = fdbEnumerator.deduceRowType(tableFieldInfo);
        return new FdbTable(schema, tableName, protoRowType, allFields);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {
        return new AbstractEnumerable<Object[]>() {
            @Override public Enumerator<Object[]> enumerator() {
                return new FdbEnumerator(schema, tableName);
            }
        };
    }
}
