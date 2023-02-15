package org.sjtu.se.ipads.fdbserver.sqlparser.adapter;

import com.apple.foundationdb.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

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

    /**
     * get all data in this table
     * @return
     */
    public List<Object[]> read() {
        List<byte[]> query_result = fdbTool.queryAll(db, tableName);
        List<Object[]> data = new ArrayList<>();
        for (byte[] elem : query_result) {
            List<String> entry = new ArrayList<>();
            int size = Tuple.fromBytes(elem).size();
            for (int i = 0; i < size; ++i) {
                entry.add(Tuple.fromBytes(elem).getString(i));
            }
            System.out.println("the entry: ");
            System.out.println(entry);
            data.add(entry.toArray());
        }
        return data;
    }


}
