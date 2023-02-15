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

    public MetaDataManager(String filePath) throws IOException {
        File file=new File(filePath);
        String content = FileUtils.readFileToString(file);
        JSONObject jsonObject = JSON.parseObject(content);
        JSONArray schemas = JSONArray.parseArray("schemas");
        int schemaSize = schemas.size();
        for (int i = 0; i < schemaSize; ++i) {
            JSONObject schema =  (JSONObject) schemas.get(i);
            JSONArray tables = JSONArray.parseArray("tables");
            int tableSize = tables.size();
            for (int j = 0; j < tableSize; ++j) {
                JSONObject table = (JSONObject) tables.get(j);
                JSONArray fields = JSONArray.parseArray("fields");
                int fieldSize = fields.size();
                List<String> entries = new ArrayList<>();
                for (int k = 0; k < fieldSize; ++k) {
                    entries.add(((JSONObject) fields.get(k)).getString("name"));
                }
                entryMap.put(table.getString("name"), entries);
            }
        }
    }

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

}
