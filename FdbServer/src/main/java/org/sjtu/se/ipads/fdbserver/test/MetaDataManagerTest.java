package org.sjtu.se.ipads.fdbserver.test;

import org.apache.calcite.util.Sources;
import org.sjtu.se.ipads.fdbserver.utils.index.MetaDataManager;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.List;

@SpringBootApplication
public class MetaDataManagerTest {
    public static void main(String[] args) {
        String filePath = Sources.of(DemoAdaptorTest.class.getResource("/demo_model.json"))
                .file().getAbsolutePath();
        try {
            MetaDataManager metaDataManager = new MetaDataManager(filePath);
            List<String> names = metaDataManager.getEntryNames("TEST1");
            System.out.println(names);

            boolean ret = metaDataManager.hasEntry("TEST2", "MESSAGE_ID");
            assert ret == true;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
