package org.sjtu.se.ipads.fdbserver.service;


import com.alibaba.fastjson.JSONObject;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.FloatValueOrBuilder;
import org.apache.calcite.util.Sources;
import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;
import org.sjtu.se.ipads.fdbserver.test.UploadServiceTest;
import org.sjtu.se.ipads.fdbserver.utils.counter.Counter;
import org.sjtu.se.ipads.fdbserver.utils.index.IndexManager;
import org.sjtu.se.ipads.fdbserver.utils.index.MetaDataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
@Scope("singleton")
public class UploadService {
    private Logger logger = LoggerFactory.getLogger(UploadService.class);
    private IndexManager indexManager;
    private MetaDataManager metaDataManager;
    private FdbTool fdbTool;
    private Database db;

    private Counter counter;

    public UploadService(String indexFile, String metaDataFile) {
        try {
            String indexPath =  "./" + indexFile;
            String metaDataPath = "./" + metaDataFile;
//            String indexPath =  Sources.of(UploadServiceTest.class.getResource(indexFile))
//                    .file().getAbsolutePath();
//            String metaDataPath = Sources.of(UploadServiceTest.class.getResource(metaDataFile))
//                    .file().getAbsolutePath();
            indexManager = new IndexManager(indexPath);
            metaDataManager = new MetaDataManager(metaDataPath);

            FDB fdb = FDB.selectAPIVersion(710);
            db = fdb.open();
            db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
            db.options().setTransactionRetryLimit(100);

            fdbTool = new FdbTool();

            // initialize the counter
            counter = new Counter(fdbTool, db, "HEADER");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("[UploadService] indexManager or metaDataManager initialization fail");
        }
    }


    public UploadService() {
        try {
            String indexPath =  "./index.json";
            String metaDataPath = "./model.json";
//            String indexPath =  Sources.of(UploadServiceTest.class.getResource("/index.json"))
//                    .file().getAbsolutePath();
//            String metaDataPath = Sources.of(UploadServiceTest.class.getResource("/model.json"))
//                    .file().getAbsolutePath();
            indexManager = new IndexManager(indexPath);
            metaDataManager = new MetaDataManager(metaDataPath);

            FDB fdb = FDB.selectAPIVersion(710);
            db = fdb.open();
            db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
            db.options().setTransactionRetryLimit(100);

            fdbTool = new FdbTool();

            // initialize the counter
            counter = new Counter(fdbTool, db, "HEADER");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("[UploadService] indexManager or metaDataManager initialization fail");
        }
    }


    /**
     * note: this conversion is not safe
     * @param val
     * @return
     */
    private int convertFloatToInt(float val) {
        val = val * 1000;
        return Math.round(val);
    }


    public boolean save(JSONObject data) {
        List<String> tables = metaDataManager.getTables();
        List<Map.Entry<String, Tuple>> toSave = new ArrayList<>();
        List<Map.Entry<String, Tuple>> toIndex = new ArrayList<>();
        boolean fail = false;

        // generate the message id
        int message_id = counter.getCounter();
        counter.addCounter();

        logger.debug("the message id for save: " + message_id);
        for (String table : tables) {
            List<String> entries = metaDataManager.getEntryNames(table);
            Tuple val = new Tuple();
            val = val.add(message_id);
            int n = entries.size();

            for (int i = 1; i < n; ++i) {
                String entry = entries.get(i);
                String type = metaDataManager.getType(table, entry);
                Object elem = data.get(entry);
                if (elem == null) {
                    fail = true;
                    break;
                } else {
                    if ((elem instanceof Float) && type.equals("int")) {
                        val = val.add(convertFloatToInt((Float) elem));
                    } else if ((elem instanceof Integer) && type.equals("int")){
                        val = val.add((Integer) elem);
                    } else if (!(elem instanceof String) && type.equals("varchar")) {
                        val = val.add(String.valueOf(elem));
                    } else if ((elem instanceof String) && type.equals("varchar")) {
                        val = val.add((String) elem);
                    }
                }

                // check index
                int isIndexed = indexManager.hasIndex(table, entry);
                if (isIndexed != -1) {
                    Tuple key = new Tuple();
                    String indexTableName = "i_" + isIndexed;
                    key = key.add(indexTableName);
//                    if (elem instanceof Integer) {
//                        key = key.add((Integer) elem);
//                    } else {
//                        key = key.add((String) elem);
//                    }
                    if ((elem instanceof Float) && type.equals("int")) {
                        key = key.add(convertFloatToInt((Float) elem));
                    } else if ((elem instanceof Integer) && type.equals("int")){
                        key = key.add((Integer) elem);
                    } else if (!(elem instanceof String) && type.equals("varchar")) {
                        key = key.add(String.valueOf(elem));
                    } else if ((elem instanceof String) && type.equals("varchar")) {
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
}
