package org.sjtu.se.ipads.fdbserver.sqlparser.adapter;

import com.apple.foundationdb.*;

import java.util.*;

import com.apple.foundationdb.tuple.Tuple;
import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;


public class FdbDataProcess {
    String tableName;
    List<LinkedHashMap<String, Object>> fields;
    FdbTool fdbTool;
    private FDB fdb;
    private Database db;

    public FdbDataProcess(FdbTableInfo tableFieldInfo) {
        fdbTool = new FdbTool();
        fields = tableFieldInfo.getFields();
        tableName = tableFieldInfo.getTableName();
        fdb = FDB.selectAPIVersion(710);
        db = fdb.open();
        db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
        db.options().setTransactionRetryLimit(100);
    }

    private List<Object> parseObject(byte[] elem) {
        int size = Tuple.fromBytes(elem).size();
        List<Object> entry = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            System.out.println(fields.get(i).get("type").toString());
            if (fields.get(i).get("type").toString().equals("int")) {
                entry.add(Tuple.fromBytes(elem).getLong(i));
            } else {
                entry.add(Tuple.fromBytes(elem).getString(i));
            }
        }
        return entry;
    }

    /**
     * get all data in this table
     * @return
     */
    public List<Object[]> read() {
        List<byte[]> query_result = fdbTool.queryAll(db, tableName);
        List<Object[]> data = new ArrayList<>();
        for (byte[] elem : query_result) {
            List<Object> entry = parseObject(elem);
//            System.out.println("the entry: ");
//            System.out.println(entry);
            data.add(entry.toArray());
        }
        return data;
    }

    /**
     * get the specific line in database
     * @param primaryKey
     * @return
     */
    public Map.Entry<Object[], Integer> readNext(int primaryKey) {
        List<Object> result;
        int key = primaryKey;
        while (true) {
            byte[] keys = Tuple.from(tableName, key).pack();
            byte[] query_result = fdbTool.query(db, keys);
            if (query_result == null) {
                return null;
            }
            result = parseObject(query_result);
            if (result != null && result.size() == 1 && result.get(0) == "DELETE") {
                ++key;
            } else {
                break;
            }
        }
        Object[] ret = result.toArray();
        return new AbstractMap.SimpleEntry<>(ret, primaryKey);
    }
}
