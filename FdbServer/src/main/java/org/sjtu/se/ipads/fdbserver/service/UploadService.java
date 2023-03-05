//package org.sjtu.se.ipads.fdbserver.service;
//
//
//import com.alibaba.fastjson.JSONObject;
//import com.apple.foundationdb.Database;
//import com.apple.foundationdb.FDB;
//import com.apple.foundationdb.FDBException;
//import com.apple.foundationdb.Transaction;
//import com.apple.foundationdb.tuple.Tuple;
//import org.apache.calcite.util.Sources;
//import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;
//import org.sjtu.se.ipads.fdbserver.test.DemoAdaptorTest;
//import org.sjtu.se.ipads.fdbserver.utils.index.IndexManager;
//import org.sjtu.se.ipads.fdbserver.utils.index.MetaDataManager;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.*;
//
//public class UploadService {
//    private Logger logger = LoggerFactory.getLogger(UploadService.class);
//    private IndexManager indexManager;
//    private MetaDataManager metaDataManager;
//    private FdbTool fdbTool;
//    private Database db;
//    private static int TIMESTAMP_LENGTH = 13;
//    private static int FLOAT_LENGTH = 8;
//    private static int INT_LENGTH = 4;
//
//    public UploadService(String indexFile, String metaDataFile) {
//        try {
//            String indexPath =  Sources.of(DemoAdaptorTest.class.getResource(indexFile))
//                    .file().getAbsolutePath();
//            System.out.println("indexPath: " + indexPath);
//            String metaDataPath = Sources.of(DemoAdaptorTest.class.getResource(metaDataFile))
//                    .file().getAbsolutePath();
//            System.out.println("metaDataPath: " + metaDataPath);
//            indexManager = new IndexManager(indexPath);
//            metaDataManager = new MetaDataManager(metaDataPath);
//
//            FDB fdb = FDB.selectAPIVersion(710);
//            db = fdb.open();
//            db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
//            db.options().setTransactionRetryLimit(100);
//
//            fdbTool = new FdbTool();
//        } catch (IOException e) {
//            e.printStackTrace();
//            logger.error("[UploadService] indexManager or metaDataManager initialization fail");
//        }
//    }
//
//
//    public UploadService() {
//        try {
//            String indexPath =  Sources.of(DemoAdaptorTest.class.getResource("/index.json"))
//                    .file().getAbsolutePath();
//            String metaDataPath = Sources.of(DemoAdaptorTest.class.getResource("/model.json"))
//                    .file().getAbsolutePath();
//            indexManager = new IndexManager(indexPath);
//            metaDataManager = new MetaDataManager(metaDataPath);
//
//            FDB fdb = FDB.selectAPIVersion(710);
//            db = fdb.open();
//            db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
//            db.options().setTransactionRetryLimit(100);
//
//            fdbTool = new FdbTool();
//        } catch (IOException e) {
//            e.printStackTrace();
//            logger.error("[UploadService] indexManager or metaDataManager initialization fail");
//        }
//    }
//
//
//    public boolean save(JSONObject data, int message_id) {
//        List<String> tables = metaDataManager.getTables();
//        List<Map.Entry<String, Tuple>> toSave = new ArrayList<>();
//        List<Map.Entry<String, Tuple>> toIndex = new ArrayList<>();
//        boolean fail = false;
//
//        for (String table : tables) {
//            List<String> entries = metaDataManager.getEntryNames(table);
//            Tuple val = new Tuple();
//            val = val.add(message_id);
//            int n = entries.size();
//            for (int i = 1; i < n; ++i) {
//                String entry = entries.get(i);
//                Object elem = data.get(entry);
//                if (elem == null) {
//                    fail = true;
//                    break;
//                } else {
//                    if (elem instanceof Integer) {
//                        val = val.add((Integer) elem);
//                    } else {
//                        val = val.add((String) elem);
//                    }
//                }
//
//                // check index
//                int isIndexed = indexManager.hasIndex(table, entry);
//                if (isIndexed != -1) {
//                    Tuple key = new Tuple();
//                    String indexTableName = "i_" + isIndexed;
//                    key = key.add(indexTableName);
//                    if (elem instanceof Integer) {
//                        key = key.add((Integer) elem);
//                    } else {
//                        key = key.add((String) elem);
//                    }
//                    if (!indexManager.isSingle(table, entry)) {
//                        key = key.add(message_id);
//                    }
//                    toIndex.add(new AbstractMap.SimpleEntry<>(table, key));
//                }
//            }
//            toSave.add(new AbstractMap.SimpleEntry<>(table, val));
//        }
//
//        if (fail) {
//            return false;
//        }
//
//        // insert all entry in a transaction
//        Transaction tr = db.createTransaction();
//        for (Map.Entry<String, Tuple> elem : toSave) {
//            byte[] primaryKey = Tuple.from(elem.getKey(), message_id).pack();
//            fdbTool.add(tr, primaryKey, elem.getValue().pack());
//        }
//        byte[] indexedVal = Tuple.from(message_id).pack();
//        for (Map.Entry<String, Tuple> elem : toIndex) {
//            fdbTool.add(tr, elem.getValue().pack(), indexedVal);
//        }
//        try {
//            tr.commit();
//            tr.close();
//        } catch (FDBException e) {
//            tr.close();
//            return false;
//        }
//        return true;
//    }
//}
