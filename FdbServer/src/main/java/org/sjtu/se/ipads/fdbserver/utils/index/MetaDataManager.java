package org.sjtu.se.ipads.fdbserver.utils.index;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MetaDataManager {
    private HashMap<String, List<String>> entryMap;
    private HashMap<String, String> typeMap;
    private List<String> tables;

    public MetaDataManager(String filePath) throws IOException {
        entryMap = new HashMap<>();
        typeMap = new HashMap<>();
        tables = new ArrayList<>();

        File file=new File(filePath);
        String content = FileUtils.readFileToString(file);
        JSONObject jsonObject = JSON.parseObject(content);
        JSONArray schemas = jsonObject.getJSONArray("schemas");
        int schemaSize = schemas.size();
        for (int i = 0; i < schemaSize; ++i) {
            JSONObject schema =  (JSONObject) schemas.get(i);
            JSONArray tables = schema.getJSONArray("tables");
            int tableSize = tables.size();
            for (int j = 0; j < tableSize; ++j) {
                JSONObject table = (JSONObject) tables.get(j);
                JSONObject operand = table.getJSONObject("operand");
                JSONArray fields = operand.getJSONArray("fields");
                int fieldSize = fields.size();
                List<String> entries = new ArrayList<>();
                String tableName = table.getString("name");
                for (int k = 0; k < fieldSize; ++k) {
                    String combine = tableName + "_" + ((JSONObject) fields.get(k)).getString("name");
                    typeMap.put(combine, ((JSONObject) fields.get(k)).getString("type"));
                    entries.add(((JSONObject) fields.get(k)).getString("name"));
                }
                entryMap.put(table.getString("name"), entries);
                this.tables.add(table.getString("name"));
            }
        }
    }

    /**
     * get the entry name list that belong to the table
     * @param name
     * @return
     */
    public List<String> getEntryNames(String name) {
        if (!entryMap.containsKey(name)) {
            return new ArrayList<>();
        } else {
            return entryMap.get(name);
        }
    }

    public boolean hasEntry(String table, String entry) {
        if (!entryMap.containsKey(table)) {
            return false;
        }
        if (!entryMap.get(table).contains(entry)) {
            return false;
        }
        return true;
    }

    public HashMap<String, List<String>> getEntryMap() {
        return entryMap;
    }

    public List<String> getTables() {
        return tables;
    }

    public String getType(String table, String entry) {
        String combine = table + "_" + entry;
        if (!typeMap.containsKey(combine)) {
            return null;
        }
        return typeMap.get(combine);
    }
}
