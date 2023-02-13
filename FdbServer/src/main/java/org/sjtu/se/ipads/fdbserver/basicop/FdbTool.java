package org.sjtu.se.ipads.fdbserver.basicop;

import com.apple.foundationdb.*;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class FdbTool {
    /**
     * insert into the dataset
     * @param db
     * @param key example: Tuple.from("class", key_str).pack()
     * @param val example: encodeInt(100)
     */
    public void add(TransactionContext db, byte[] key, byte[] val) {
        db.run((Transaction tr) -> {
            tr.set(key, val);
            return null;
        });
    }

    /**
     * query the value for the key
     * @param db
     * @param key
     * @return
     */
    public byte[] query(TransactionContext db, final byte[] key) {
        return db.run((Transaction tr) -> {
            return tr.get(key).join();
        });
    }

    /**
     * delete the k-v pair from the dataset
     * @param db
     * @param key
     */
    public void remove(TransactionContext db, final byte[] key) {
        db.run((Transaction tr) -> {
            tr.clear(key);
            return null;
        });
    }

    /**
     * get all data in a table
     * @param db
     * @param table_name
     * @return
     */
    public List<byte[]> queryAll(TransactionContext db, String table_name) {
        return db.run((Transaction tr) -> {
            List<KeyValue> kvs = tr.getRange(Tuple.from(table_name).range()).asList().join();
            List<byte[]> result = new ArrayList<>();
            for (KeyValue kv : kvs) {
                result.add(kv.getValue());
            }
            return result;
        });
    }
    // TODO: get a range of data and delete a range of data

}
