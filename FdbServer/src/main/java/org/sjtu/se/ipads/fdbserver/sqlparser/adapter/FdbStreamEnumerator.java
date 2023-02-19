package org.sjtu.se.ipads.fdbserver.sqlparser.adapter;

import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.commons.lang3.time.FastDateFormat;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;

public class FdbStreamEnumerator<E> implements Enumerator<Object[]> {
    List<String> filterValues;
    private final AtomicBoolean cancelFlag;
    private final RowConverter<Object[]> rowConverter;
    private Object[] currentObj;
    static int nextPrimaryKey = 1;
    FdbDataProcess dataProcess;

    public FdbStreamEnumerator(FdbSchema schema, String tableName, AtomicBoolean cancelFlag, RowConverter<E> rowConverter) {
        FdbTableInfo tableFieldInfo = schema.getTableFieldInfo(tableName);
        dataProcess = new FdbDataProcess(tableFieldInfo);
        this.rowConverter = (RowConverter<Object[]>) rowConverter;
        this.filterValues = filterValues == null ? null
                : ImmutableNullableList.copyOf(filterValues);
        this.cancelFlag = cancelFlag;
    }


    /** Returns an array of integers {0, ..., n - 1}. */
    public static int[] identityList(int n) {
        int[] integers = new int[n];
        for (int i = 0; i < n; i++) {
            integers[i] = i;
        }
        return integers;
    }

    private static RelDataType toNullableRelDataType(JavaTypeFactory typeFactory,
                                                     SqlTypeName sqlTypeName) {
        return typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlTypeName), true);
    }

    private static final FastDateFormat TIME_FORMAT_TIMESTAMP;

    static {
        final TimeZone gmt = TimeZone.getTimeZone("GMT");
        TIME_FORMAT_TIMESTAMP =
                FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", gmt);
    }

    @Override
    public Object[] current() {
        if (currentObj != null) {
            return currentObj;
        }
        Object[] current = null;
        while (current == null) {
           Map.Entry<Object[], Integer> ret = dataProcess.readNext(nextPrimaryKey);
           current = ret.getKey();
           nextPrimaryKey = ret.getValue() + 1;
        }
        return current;
    }

    @Override
    public boolean moveNext() {
        int retryTime = 0;
        currentObj = null;
        Map.Entry<Object[], Integer> ret = dataProcess.readNext(nextPrimaryKey + retryTime);
        ++retryTime;
        currentObj = ret.getKey();
        nextPrimaryKey = ret.getValue() + 1;

        if (currentObj != null) {
            return true;
        } else {
           return false;
        }
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {

    }

    private static RowConverter<?> converter(List<RelDataType> fieldTypes,
                                             List<Integer> fields) {
        if (fields.size() == 1) {
            final int field = fields.get(0);
            return new SingleColumnRowConverter(fieldTypes.get(field), field);
        } else {
            return arrayConverter(fieldTypes, fields, false);
        }
    }

    public static RowConverter<Object[]> arrayConverter(
            List<RelDataType> fieldTypes, List<Integer> fields, boolean stream) {
        return new ArrayRowConverter(fieldTypes, fields, stream);
    }

    /** Row converter.
     *
     * @param <E> element type */
    abstract static class RowConverter<E> {
        abstract E convertRow(String[] rows);

        @SuppressWarnings("JavaUtilDate")
        protected Object convert(RelDataType fieldType, String string) {
            if (fieldType == null || string == null) {
                return string;
            }
            switch (fieldType.getSqlTypeName()) {
                case BOOLEAN:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Boolean.parseBoolean(string);
                case TINYINT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Byte.parseByte(string);
                case SMALLINT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Short.parseShort(string);
                case INTEGER:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Integer.parseInt(string);
                case BIGINT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Long.parseLong(string);
                case FLOAT:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Float.parseFloat(string);
                case DOUBLE:
                    if (string.length() == 0) {
                        return null;
                    }
                    return Double.parseDouble(string);
                case DECIMAL:
                    if (string.length() == 0) {
                        return null;
                    }
                    return parseDecimal(fieldType.getPrecision(), fieldType.getScale(), string);
                case TIMESTAMP:
                    if (string.length() == 0) {
                        return null;
                    }
                    try {
                        Date date = TIME_FORMAT_TIMESTAMP.parse(string);
                        return date.getTime();
                    } catch (ParseException e) {
                        return null;
                    }
                case VARCHAR:
                default:
                    return string;
            }
        }
    }

    private static RelDataType parseDecimalSqlType(JavaTypeFactory typeFactory, int precision,
                                                   int scale) {
        checkArgument(precision > 0, "DECIMAL type must have precision > 0. Found %s", precision);
        checkArgument(scale >= 0, "DECIMAL type must have scale >= 0. Found %s", scale);
        checkArgument(precision >= scale,
                "DECIMAL type must have precision >= scale. Found precision (%s) and scale (%s).",
                precision, scale);
        return typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale), true);
    }

    @VisibleForTesting
    protected static BigDecimal parseDecimal(int precision, int scale, String string) {
        BigDecimal result = new BigDecimal(string);
        // If the parsed value has more fractional digits than the specified scale, round ties away
        // from 0.
        if (result.scale() > scale) {
            result = result.setScale(scale, RoundingMode.HALF_UP);
        }
        // Throws an exception if the parsed value has more digits to the left of the decimal point
        // than the specified value.
        if (result.precision() - result.scale() > precision - scale) {
            throw new IllegalArgumentException(String
                    .format(Locale.ROOT, "Decimal value %s exceeds declared precision (%d) and scale (%d).",
                            result, precision, scale));
        }
        return result;
    }

    /** Array row converter. */
    static class ArrayRowConverter extends RowConverter<Object[]> {

        /** Field types. List must not be null, but any element may be null. */
        private final List<RelDataType> fieldTypes;
        private final ImmutableIntList fields;
        /** Whether the row to convert is from a stream. */
        private final boolean stream;

        ArrayRowConverter(List<RelDataType> fieldTypes, List<Integer> fields,
                          boolean stream) {
            this.fieldTypes = ImmutableNullableList.copyOf(fieldTypes);
            this.fields = ImmutableIntList.copyOf(fields);
            this.stream = stream;
        }

        @Override public Object[] convertRow(String[] strings) {
            if (stream) {
                return convertStreamRow(strings);
            } else {
                return convertNormalRow(strings);
            }
        }

        public Object[] convertNormalRow(String[] strings) {
            final Object[] objects = new Object[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                int field = fields.get(i);
                objects[i] = convert(fieldTypes.get(field), strings[field]);
            }
            return objects;
        }

        public Object[] convertStreamRow(String[] strings) {
            final Object[] objects = new Object[fields.size() + 1];
            objects[0] = System.currentTimeMillis();
            for (int i = 0; i < fields.size(); i++) {
                int field = fields.get(i);
                objects[i + 1] = convert(fieldTypes.get(field), strings[field]);
            }
            return objects;
        }
    }

    /** Single column row converter. */
    private static class SingleColumnRowConverter extends RowConverter<Object> {
        private final RelDataType fieldType;
        private final int fieldIndex;

        private SingleColumnRowConverter(RelDataType fieldType, int fieldIndex) {
            this.fieldType = fieldType;
            this.fieldIndex = fieldIndex;
        }

        @Override public Object convertRow(String[] strings) {
            return convert(fieldType, strings[fieldIndex]);
        }
    }
}
