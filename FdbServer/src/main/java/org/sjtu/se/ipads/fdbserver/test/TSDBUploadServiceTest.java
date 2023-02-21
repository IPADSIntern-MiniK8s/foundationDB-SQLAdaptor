package org.sjtu.se.ipads.fdbserver.test;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.subspace.Subspace;
import org.apache.calcite.util.Sources;
import org.apache.commons.io.FileUtils;
import org.sjtu.se.ipads.fdbserver.tsdb.TSDBUploadService;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;

@SpringBootApplication
public class TSDBUploadServiceTest {
    public static void main(String[] args) throws IOException, InterruptedException {
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

        TSDBUploadService tsdbUploadService = new TSDBUploadService();

        String filePath = Sources.of(DemoAdaptorTest.class.getResource("/batch_input.json"))
                .file().getAbsolutePath();
        File file=new File(filePath);
        String content = FileUtils.readFileToString(file);
        JSONArray array = JSON.parseArray(content);
        int size = array.size();

        for (int i = 0; i < size; ++i) {
            JSONObject jsonObject = array.getJSONObject(i);

            boolean ret = tsdbUploadService.save(jsonObject);
            if (ret != true) throw new AssertionError();

            // check data in log file
//            tsdbUploadService.getLogManager().restoreLog();
//            System.out.println("the log size: " + tsdbUploadService.getLogManager().getRestoreEntries().size());

            // Simulate the car to send information, pause for one second
            Thread.sleep(1000);
        }


        // query some insertion for check

    }
}
