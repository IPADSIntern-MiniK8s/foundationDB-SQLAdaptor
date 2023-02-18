package org.sjtu.se.ipads.fdbserver.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.calcite.util.Sources;
import org.apache.commons.io.FileUtils;
import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;
import org.sjtu.se.ipads.fdbserver.service.UploadService;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class UploadServiceTest {
    public static void main(String[] args) throws IOException {
        FDB fdb = FDB.selectAPIVersion(710);
        Database db = fdb.open();
        db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
        db.options().setTransactionRetryLimit(100);


        // clean before
        db.run(tx -> {
            final byte[] st = new Subspace(new byte[]{(byte) 0x00}).getKey();
            final byte[] en = new Subspace(new byte[]{(byte) 0xFF}).getKey();
            tx.clear(st, en);
            return null;
        });

        FdbTool fdbTool = new FdbTool();

        UploadService uploadService = new UploadService();

        String filePath = Sources.of(DemoAdaptorTest.class.getResource("/input.json"))
                .file().getAbsolutePath();
        File file=new File(filePath);
        String content = FileUtils.readFileToString(file);
        JSONObject jsonObject = JSON.parseObject(content);

        boolean ret = uploadService.save(db, jsonObject, 1);
        assert ret == true;

        // test save result by querying
        List<String> tables = Arrays.asList("HEADER", "GEOMETRY", "IMAGE");
        for (String table : tables) {
            byte[] key = Tuple.from(table, 1).pack();
            byte[] result = fdbTool.query(db, key);
            List<Object> entry = new ArrayList<>();
            int size = Tuple.fromBytes(result).size();
            for (int i = 0; i < size; ++i) {
                entry.add(Tuple.fromBytes(result).get(i));
            }
            System.out.println(entry);
        }

        List<String> indexTables = Arrays.asList("i_0", "i_1", "i_2");
        List<byte[]> results = fdbTool.queryRange(db, indexTables.get(0), 1, 2);
        System.out.println("result size: " + results.size());
        assert results.size() == 1;
        byte[] key = Tuple.from(indexTables.get(0), 1, 1).pack();
        byte[] result = fdbTool.query(db, key);
        assert Tuple.fromBytes(result).getLong(0) == 1;
        key = Tuple.from(indexTables.get(1), "2277166961600969300", 1).pack();
        result = fdbTool.query(db, key);
        assert Tuple.fromBytes(result).getLong(0) == 1;
        key = Tuple.from(indexTables.get(2), 10344450, 1).pack();
        result = fdbTool.query(db, key);
        assert Tuple.fromBytes(result).getLong(0) == 1;
    }
}
