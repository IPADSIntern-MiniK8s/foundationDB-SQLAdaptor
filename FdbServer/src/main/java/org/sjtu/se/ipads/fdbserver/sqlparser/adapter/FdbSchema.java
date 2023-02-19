package org.sjtu.se.ipads.fdbserver.sqlparser.adapter;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.calcite.model.JsonCustomTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.*;
import java.util.stream.Collectors;

public class FdbSchema extends AbstractSchema {
    public final List<Map<String, Object>> tables;
    private Map<String, Table> tableMap = null;
    public final int database;
    private String flavorName;

    public FdbSchema(int database,
                     List<Map<String, Object>> tables, String flavorName) {
        this.database = database;
        this.tables = tables;
        this.flavorName = flavorName;
    }

    private Table table(String tableName) {
        if (Objects.equals(flavorName, "scannable")) {
            return FdbTable.create(FdbSchema.this, tableName,null);
        } else {
            Map<String, Object> allFields = null;
            for (int i = 0; i < this.tables.size(); i++) {
                JsonCustomTable jsonCustomTable = (JsonCustomTable) this.tables.get(i);
                if (jsonCustomTable.name.equals(tableName)) {
                    allFields = this.tables.get(i);
                    break;
                }
            }
            return new FdbStreamTable(FdbSchema.this, tableName, null, allFields);
        }
    }

    @Override
    protected Map<String, Table> getTableMap() {
        JsonCustomTable[] jsonCustomTables = new JsonCustomTable[tables.size()];
        Set<String> tableNames = Arrays.stream(tables.toArray(jsonCustomTables))
                .map(e -> e.name).collect(Collectors.toSet());
        tableMap = Maps.asMap(
                ImmutableSet.copyOf(tableNames),
                CacheBuilder.newBuilder()
                        .build(CacheLoader.from(this::table)));
        return tableMap;
    }

    public FdbTableInfo getTableFieldInfo(String tableName) {
        FdbTableInfo tableFieldInfo = new FdbTableInfo();
        List<LinkedHashMap<String, Object>> fields = new ArrayList<>();
        Map<String, Object> map;
        String keyDelimiter = "";
        for (int i = 0; i < this.tables.size(); i++) {
            JsonCustomTable jsonCustomTable = (JsonCustomTable) this.tables.get(i);
            if (jsonCustomTable.name.equals(tableName)) {
                map = jsonCustomTable.operand;
                if (map.get("fields") == null) {
                    throw new RuntimeException("fields is null");
                }
                fields = (List<LinkedHashMap<String, Object>>) map.get("fields");
                if (map.get("keyDelimiter") != null) {
                    keyDelimiter = map.get("keyDelimiter").toString();
                }
                break;
            }
        }
        tableFieldInfo.setTableName(tableName);
        tableFieldInfo.setFields(fields);
        if (!keyDelimiter.isEmpty()) {
            tableFieldInfo.setKeyDelimiter(keyDelimiter);
        }
        return tableFieldInfo;
    }
}
