package org.sjtu.se.ipads.fdbserver.sqlparser.adapter;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;

import java.util.Map;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Source;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Table based on a CSV file.
 *
 * <p>It implements the {@link ScannableTable} interface, so Calcite gets
 * data by calling the {@link #scan(DataContext)} method.
 */
public class FdbStreamTable extends FdbTable
        implements StreamableTable {
    /** Creates a FdbStreamTable . */
    FdbStreamTable (FdbSchema schema, String tableName, RelProtoDataType protoRowType, Map<String, Object> allFields) {
        super(schema, tableName, protoRowType, allFields);
    }

    protected boolean isStream() {
        return true;
    }

    @Override public String toString() {
        return "FdbStreamTable";
    }


    @Override public Enumerable<Object[]> scan(DataContext root) {
        JavaTypeFactory typeFactory = root.getTypeFactory();
        final List<RelDataType> fieldTypes = getFieldTypes(typeFactory);
        final List<Integer> fields = ImmutableIntList.identity(fieldTypes.size());
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<Object[]>() {
            @Override public Enumerator<Object[]> enumerator() {
                return new FdbStreamEnumerator<Object[]>(schema, tableName, cancelFlag,
                        FdbStreamEnumerator.arrayConverter(fieldTypes, fields, true));
            }
        };
    }

    @Override public Table stream() {
        return this;
    }
}
