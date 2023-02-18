package org.sjtu.se.ipads.fdbserver.test;

import org.apache.calcite.util.Sources;
import org.sjtu.se.ipads.fdbserver.utils.index.IndexManager;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class IndexManagerTest {
    public static void main(String[] args) {
        try {
            String filePath =  Sources.of(DemoAdaptorTest.class.getResource("/index.json"))
                    .file().getAbsolutePath();
            IndexManager indexManager = new IndexManager(filePath);

            // test `hasIndex` api
            int ret = indexManager.hasIndex("HEADER", "CAR_ID");
            assert ret == 0;
            ret = indexManager.hasIndex("GEOMETRY", "X");
            assert ret == 2;
            ret = indexManager.hasIndex("non", "Y");
            assert ret == -1;

            // test `isSingle` api
            boolean single = indexManager.isSingle("HEADER", "TIMESTAMP");
            assert single == false;

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
