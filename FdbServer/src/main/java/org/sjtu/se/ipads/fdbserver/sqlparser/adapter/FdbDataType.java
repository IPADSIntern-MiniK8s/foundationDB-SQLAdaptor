package org.sjtu.se.ipads.fdbserver.sqlparser.adapter;
import java.sql.Timestamp;


/**
 * All available data type for Fdb.
 */
public enum FdbDataType {

    /**
     * Strings are the most basic kind of value. Redis Strings are binary safe,
     * this means that a Redis string can contain any kind of data, for instance a JPEG image
     * or a serialized Ruby object.
     */
    STRING("string"),

    INT("int"),
    /**
     * Lists are simply lists of strings, sorted by insertion order.
     */
    LIST("list");



    private final String typeName;

    FdbDataType(String typeName) {
        this.typeName = typeName;
    }

    public static FdbDataType fromTypeName(String typeName) {
        for (FdbDataType type : FdbDataType.values()) {
            if (type.getTypeName().equals(typeName)) {
                return type;
            }
        }
        return null;
    }

    public String getTypeName() {
        return this.typeName;
    }
}
