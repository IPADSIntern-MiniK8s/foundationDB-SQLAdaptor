package org.sjtu.se.ipads.fdbserver.utils.index;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class IndexManager {
    private HashMap<String, Set<String>> indexMap;
    private HashSet<String> singleIndex;
    private HashMap<String, Integer> indexDict;    // Save the number of the index table


    IndexManager(String indexPath) throws IOException {
        File file=new File(indexPath);
        String content = FileUtils.readFileToString(file);
        JSONObject jsonObject = JSON.parseObject(content);
        JSONArray indexes = jsonObject.getJSONArray("indexes");

        // get content and store in index map
        int n = indexes.size();
        for (int i = 0; i < n; ++i) {
            JSONObject elem = (JSONObject) indexes.get(i);
            if (indexMap.containsKey(elem.getString("table_name"))) {
                indexMap.get(elem.getString("table_name")).add(elem.getString("name"));
                String combine_name = elem.getString("table_name") + "_" + elem.getString("name");
                if (elem.getString("attribute") == "single") {
                    singleIndex.add(combine_name);
                }
                indexDict.put(combine_name, i);
            }
        }
    }


    /**
     * check where has the index for the entry and return the index number
     * @param table_name
     * @param entry
     * @return
     */
    public int hasIndex(String table_name, String entry) {
        if (!indexMap.containsKey(table_name)) {
            return -1;
        }
        if (!indexMap.get(table_name).contains(entry)) {
            return -1;
        }
        return indexDict.get(table_name + "_" + entry);
    }

    public boolean isSingle(String table_name, String entry) {
        String combine_name = table_name + "_" + entry;
        return singleIndex.contains(combine_name);
    }

}
