package org.sjtu.se.ipads.fdbserver.test;

import com.apple.foundationdb.*;
import com.apple.foundationdb.tuple.Tuple;
import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

@SpringBootApplication
public class BasicOpTest {
    public static void main(String[] args) {
        // initialize the foundationDB
        FDB fdb = FDB.selectAPIVersion(710);
        Database db = fdb.open();
        db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
        db.options().setTransactionRetryLimit(100);

        FdbTool fdbTool = new FdbTool();
        String table_name = "test";

        // test insert
        db.run((Transaction tr) -> {
            tr.clear(Tuple.from(table_name).range());
            return null;
        });
        for (long i = 0; i < 10; ++i) {
            byte[] key = Tuple.from(table_name,  Long.toString(i)).pack();
            fdbTool.add(db, key, Tuple.from(Long.toString(i)).pack());
        }

        // test query
        System.out.println("test query");
        for (long i = 0; i < 10; ++i) {
            byte[] key = Tuple.from(table_name, Long.toString(i)).pack();
            byte[] result = fdbTool.query(db, key);
            System.out.println(Tuple.fromBytes(result).getString(0));
        }

        // test delete
        System.out.println("test delete");
        for (long i = 0; i < 10; i += 2) {
            byte[] key = Tuple.from(table_name, Long.toString(i)).pack();
            fdbTool.remove(db, key);
        }

        for (long i = 0; i < 10; ++i) {
            byte[] key = Tuple.from(table_name, Long.toString(i)).pack();
            byte[] result = fdbTool.query(db, key);
            if (result == null) {
                System.out.println("key " + i + " is empty ");
                continue;
            }
            System.out.println(Tuple.fromBytes(result).getString(0));
        }

        // test query all
        System.out.println("test query all");
        List<byte[]> result = fdbTool.queryAll(db, table_name);
        System.out.print("the result size: " + result.size() + "\n");
        for (byte[] elem : result) {
            System.out.println(Tuple.fromBytes(elem).getString(0));
        }

        System.out.println("finish test");

    }
}
