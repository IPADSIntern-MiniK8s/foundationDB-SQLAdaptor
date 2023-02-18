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
    private HashSet<String> singleIndex;
    private HashMap<String, Integer> indexDict;    // Save the number of the index table


    public IndexManager(String indexPath) throws IOException {
        File file=new File(indexPath);
        String content = FileUtils.readFileToString(file);
        JSONObject jsonObject = JSON.parseObject(content);
        JSONArray indexes = jsonObject.getJSONArray("indexes");

        // initialize
        singleIndex = new HashSet<>();
        indexDict = new HashMap<>();

        // get content and store in index map
        int n = indexes.size();
        for (int i = 0; i < n; ++i) {
            JSONObject elem = (JSONObject) indexes.get(i);

            String combineName = elem.getString("table_name") + "_" + elem.getString("name");
            if (elem.getString("attribute") == "single") {
                singleIndex.add(combineName);
            }
            indexDict.put(combineName, i);
        }
    }


    /**
     * check where has the index for the entry and return the index number
     * @param table_name
     * @param entry
     * @return
     */
    public int hasIndex(String table_name, String entry) {
        String combineName = table_name + "_" + entry;
        if (indexDict.containsKey(combineName)) {
            return indexDict.get(combineName);
        }
        return -1;
    }

    /**
     * Whether the primary key corresponding to the index is unique
     * @param table_name
     * @param entry
     * @return
     */
    public boolean isSingle(String table_name, String entry) {
        String combine_name = table_name + "_" + entry;
        return singleIndex.contains(combine_name);
    }

}
