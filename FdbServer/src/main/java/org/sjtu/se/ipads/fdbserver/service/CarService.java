package org.sjtu.se.ipads.fdbserver.service;

import com.alibaba.fastjson.JSONObject;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.calcite.util.Sources;
import org.sjtu.se.ipads.fdbserver.FdbServerApplication;
import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;
import org.sjtu.se.ipads.fdbserver.utils.index.IndexManager;
import org.sjtu.se.ipads.fdbserver.utils.index.MetaDataManager;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class CarService {

    private FdbTool fdbTool;
    private Database db;

    private IndexManager indexManager;
    private MetaDataManager metaDataManager;
    public boolean uploadData(
            long time_stamp,int car_id,double x,double y,double r,double vx,double vy,double vr,String img
    ){

        JSONObject data = new JSONObject();
        data.put("TIME_STAMP",(Object)time_stamp);
        data.put("CAR_ID",(Object)car_id);
        data.put("X",(Object)x);
        data.put("Y",(Object)y);
        data.put("V_X",(Object)vx);
        data.put("V_Y",(Object)vy);
        data.put("V_R",(Object)vr);
        data.put("DIRECTION",(Object)r);
        data.put("IMG",(Object)img);
        String message_id = String.valueOf(UUID.randomUUID());
        List<String> tables = metaDataManager.getTables();
        List<Map.Entry<String, Tuple>> toSave = new ArrayList<>();
        List<Map.Entry<String, Tuple>> toIndex = new ArrayList<>();
        boolean fail = false;

        for (String table : tables) {
            List<String> entries = metaDataManager.getEntryNames(table);
            Tuple val = new Tuple();
            val = val.add(message_id);
            int n = entries.size();
            for (int i = 1; i < n; ++i) {
                String entry = entries.get(i);
                Object elem = data.get(entry);
                if (elem == null) {
                    fail = true;
                    break;
                } else {
                    if (elem instanceof Integer) {
                        val = val.add((Integer) elem);
                    } else {
                        val = val.add((String) elem);
                    }
                }

                // check index
                int isIndexed = indexManager.hasIndex(table, entry);
                if (isIndexed != -1) {
                    Tuple key = new Tuple();
                    String indexTableName = "i_" + isIndexed;
                    key = key.add(indexTableName);
                    if (elem instanceof Integer) {
                        key = key.add((Integer) elem);
                    } else {
                        key = key.add((String) elem);
                    }
                    if (!indexManager.isSingle(table, entry)) {
                        key = key.add(message_id);
                    }
                    toIndex.add(new AbstractMap.SimpleEntry<>(table, key));
                }
            }
            toSave.add(new AbstractMap.SimpleEntry<>(table, val));
        }

        if (fail) {
            return false;
        }

        // insert all entry in a transaction
        Transaction tr = db.createTransaction();
        for (Map.Entry<String, Tuple> elem : toSave) {
            byte[] primaryKey = Tuple.from(elem.getKey(), message_id).pack();
            fdbTool.add(tr, primaryKey, elem.getValue().pack());
        }
        byte[] indexedVal = Tuple.from(message_id).pack();
        for (Map.Entry<String, Tuple> elem : toIndex) {
            fdbTool.add(tr, elem.getValue().pack(), indexedVal);
        }
        try {
            tr.commit();
            tr.close();
        } catch (FDBException e) {
            tr.close();
            return false;
        }
        return true;

    }




    public CarService() throws IOException {

        String indexPath =  "./index.json";
        String metaDataPath = "./model.json";
//        String indexPath = this.getClass().getClassLoader().getResource("index.json").getPath();
//        String metaDataPath = this.getClass().getClassLoader().getResource("model.json").getPath();
        indexManager = new IndexManager(indexPath);
        metaDataManager = new MetaDataManager(metaDataPath);

        FDB fdb = FDB.selectAPIVersion(710);
        db = fdb.open();
        db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
        db.options().setTransactionRetryLimit(100);

        fdbTool = new FdbTool();
    }
}
