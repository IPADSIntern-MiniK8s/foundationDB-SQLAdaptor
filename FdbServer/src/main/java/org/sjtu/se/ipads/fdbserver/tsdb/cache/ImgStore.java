package org.sjtu.se.ipads.fdbserver.tsdb.cache;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;
import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ImgStore implements Runnable {
    private int carId;
    private long timestamp;
    private String img;
    private static List<Map.Entry<String, String>> tags;
    private static String measurement;

    static {
        measurement = "";
    }

    public ImgStore() {}

    // TODO: the metadata need to be read from the model file
    public ImgStore(int carId, long timestamp, String img) {
        this.carId = carId;
        this.timestamp = timestamp;
        this.img = img;

        if (measurement.isEmpty()) {
            measurement = "CAR";
            tags = new ArrayList<>();
            tags.add(new AbstractMap.SimpleEntry<>("CAR_ID", "int"));
        }
    }

    public void run() {
        FDB fdb = FDB.selectAPIVersion(710);
        Database db = fdb.open();
        db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
        db.options().setTransactionRetryLimit(100);

        FdbTool fdbTool = new FdbTool();

        // key: (measurement, tag set, start timestamp)
        Tuple keyTuple = new Tuple();
        keyTuple = keyTuple.add(measurement);
        keyTuple = keyTuple.add(tags.get(0).getKey());
        keyTuple = keyTuple.add(carId);
        keyTuple = keyTuple.add(timestamp);
        byte[] key = keyTuple.pack();

        // value: the img
        byte[] value = Tuple.from(img).pack();
        fdbTool.add(db, key, value);
    }
}
