package org.sjtu.se.ipads.fdbserver.tsdb;


import com.alibaba.fastjson.JSONObject;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.calcite.util.Sources;
import org.sjtu.se.ipads.fdbserver.test.DemoAdaptorTest;
import org.sjtu.se.ipads.fdbserver.tsdb.cache.CacheManager;
import org.sjtu.se.ipads.fdbserver.tsdb.log.LogEntry;
import org.sjtu.se.ipads.fdbserver.tsdb.log.LogManager;
import org.sjtu.se.ipads.fdbserver.tsdb.log.LogType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// TODO: maybe need to use config instead of hard code
public class TSDBUploadService {
    private Logger logger = LoggerFactory.getLogger(TSDBUploadService.class);
    private CacheManager cacheManager;
    private LogManager logManager;

    public TSDBUploadService() {
        int clientCount = 4;
        String path = this.getClass().getClassLoader().getResource("").getPath();
        path += "log/";
        cacheManager = new CacheManager(clientCount);
        logManager = new LogManager(path, 5, true);
    }

    public LogManager getLogManager() {
        return logManager;
    }

    public boolean save(JSONObject data) {
        // TODO: the first version: hard code
        String timeStampName = "TIME_STAMP";
        String tagName = "CAR_ID";
        List<String> fields = Arrays.asList("X", "Y", "V_X", "V_Y", "V_R", "DIRECTION");
        String img;
        String imgName = "IMG";

        boolean fail = false;
        String timestamp = data.getString(timeStampName);
        if (timestamp == null) {
            return false;
        }

        Integer tag = data.getInteger(tagName);
        if (tag == null) {
            return false;
        }

        Tuple fieldTuple = new Tuple();
        List<Integer> fieldList = new ArrayList<>();
        for (String field : fields) {
            Object obj = data.get(field);
            if (obj == null) {
                return false;
            }
            fieldTuple = fieldTuple.add((int) obj);
            fieldList.add((int) obj);
        }

        img = data.getString("IMG");

        // write ahead log
        LogEntry newEntry = new LogEntry(LogType.INSERT, timestamp, tag, fieldList, img);
        logManager.appendLog(newEntry);

        // save to the cache
        boolean ret = cacheManager.insertToCache(timestamp, fieldTuple, tag, img);

        if (ret) {
            // check whether it needs to flush
            cacheManager.triggerFlush();
        }
        return ret;
    }
}
