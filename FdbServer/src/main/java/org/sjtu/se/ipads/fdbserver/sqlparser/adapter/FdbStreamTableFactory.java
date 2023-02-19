package org.sjtu.se.ipads.fdbserver.sqlparser.adapter;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

public class FdbStreamTableFactory implements TableFactory {
    @SuppressWarnings("unused")
    public static final FdbStreamTableFactory INSTANCE = new FdbStreamTableFactory();

    private FdbStreamTableFactory() {
    }

    // name that is also the same name as a complex metric
    @Override public Table create(SchemaPlus schema, String tableName, Map operand, RelDataType rowType) {
        final FdbSchema fdbSchema = schema.unwrap(FdbSchema.class);
        final RelProtoDataType protoRowType =
                rowType != null ? RelDataTypeImpl.proto(rowType) : null;
        return FdbStreamTable.create(fdbSchema, tableName, operand, protoRowType);
    }
}
