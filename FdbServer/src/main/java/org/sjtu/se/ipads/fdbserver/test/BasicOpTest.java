//package org.sjtu.se.ipads.fdbserver.test;
//
//import com.apple.foundationdb.*;
//import com.apple.foundationdb.tuple.Tuple;
//import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//
//import java.util.Arrays;
//import java.util.LinkedHashMap;
//import java.util.List;
//
//@SpringBootApplication
//public class BasicOpTest {
//    public static void main(String[] args) {
//        // initialize the foundationDB
//        FDB fdb = FDB.selectAPIVersion(710);
//        Database db = fdb.open();
//        db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
//        db.options().setTransactionRetryLimit(100);
//
//        FdbTool fdbTool = new FdbTool();
//        String table_name = "test";
//
//        // test insert
//        db.run((Transaction tr) -> {
//            tr.clear(Tuple.from(table_name).range());
//            return null;
//        });
//        for (long i = 0; i < 10; ++i) {
//            byte[] key = Tuple.from(table_name, i).pack();
//            fdbTool.add(db, key, Tuple.from(i).pack());
//        }
//
//        for (long i = 10; i < 20; ++i) {
//            byte[] key = Tuple.from(table_name, i, i - 10).pack();
//            fdbTool.add(db, key, Tuple.from(i - 10).pack());
//        }
//
//        // test query
//        System.out.println("test query");
//        for (long i = 0; i < 10; ++i) {
//            byte[] key = Tuple.from(table_name, i).pack();
//            byte[] result = fdbTool.query(db, key);
//            System.out.println(Tuple.fromBytes(result).getLong(0));
//        }
//
//        // test delete
//        System.out.println("test delete");
//        for (long i = 0; i < 10; i += 2) {
//            byte[] key = Tuple.from(table_name, i).pack();
//            fdbTool.remove(db, key);
//        }
//
//        for (long i = 0; i < 10; ++i) {
//            byte[] key = Tuple.from(table_name, i).pack();
//            byte[] result = fdbTool.query(db, key);
//            if (result == null) {
//                System.out.println("key " + i + " is empty ");
//                continue;
//            }
//            System.out.println(Tuple.fromBytes(result).getLong(0));
//        }
//
//        // test query all
//        System.out.println("test query all");
//        List<byte[]> result = fdbTool.queryAll(db, table_name);
//        System.out.print("the result size: " + result.size() + "\n");
//        for (byte[] elem : result) {
//            System.out.println(Tuple.fromBytes(elem).getLong(0));
//        }
//
//        // test query range
//        System.out.println("test query range");
//        result = fdbTool.queryRange(db, table_name, 10, 15);
//        System.out.print("the result size: " + result.size() + "\n");
//        for (byte[] elem : result) {
//            System.out.println(Tuple.fromBytes(elem).getLong(0));
//        }
//
//        System.out.println("finish test");
//    }
//}
