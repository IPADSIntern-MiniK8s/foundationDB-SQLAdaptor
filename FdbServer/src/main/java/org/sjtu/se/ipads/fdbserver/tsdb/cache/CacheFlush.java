package org.sjtu.se.ipads.fdbserver.tsdb.cache;

import com.apple.foundationdb.*;
import com.apple.foundationdb.tuple.Tuple;
import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;
import org.sjtu.se.ipads.fdbserver.tsdb.dataprocessor.fieldProto;

import java.util.*;

class CacheFlush implements Runnable {
    private int carId;
    private long startTimestamp;
    private Map<Integer, Tuple> data;
    private static int ATTRIBUTE_COUNT = 6;
    private static String measurement;
    private static List<Map.Entry<String, String>> fields;
    private static List<Map.Entry<String, String>> tags;

    static {
        measurement = "";
    }


    public CacheFlush(int carId, long startTimestamp, Map<Integer, Tuple> data) {
        this.carId = carId;
        this.startTimestamp = startTimestamp;
        this.data = data;

        // TODO: the metadata need to be read from the model file
        if (measurement.isEmpty()) {
            measurement = "CAR";
            fields = new ArrayList<>();
            fields.add(new AbstractMap.SimpleEntry<>("X", "int"));
            fields.add(new AbstractMap.SimpleEntry<>("Y", "int"));
            fields.add(new AbstractMap.SimpleEntry<>("V_X", "int"));
            fields.add(new AbstractMap.SimpleEntry<>("V_Y", "int"));
            fields.add(new AbstractMap.SimpleEntry<>("V_R", "int"));
            fields.add(new AbstractMap.SimpleEntry<>("DIRECTION", "int"));
            ATTRIBUTE_COUNT = fields.size();

            tags = new ArrayList<>();
            tags.add(new AbstractMap.SimpleEntry<>("CAR_ID", "int"));
        }
    }

    /**
     * first version: only one thread, flush the data to bottom database
     * TODO: need be replaced by hadoop mapreduce
     */
    public void run() {
        FDB fdb = FDB.selectAPIVersion(710);
        Database db = fdb.open();
        db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
        db.options().setTransactionRetryLimit(100);

        FdbTool fdbTool = new FdbTool();

        // pack the data
        List<fieldProto.fieldList.Builder> builders = new ArrayList<>();
        for (int i = 0; i < ATTRIBUTE_COUNT; ++i) {
            builders.add(fieldProto.fieldList.newBuilder());
        }

        Iterator<Map.Entry<Integer, Tuple>> entries = data.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<Integer, Tuple> entry = entries.next();
            Integer key = entry.getKey();
            Tuple value = entry.getValue();

            // this entry is illegal, discard it
            if (value.size() != ATTRIBUTE_COUNT) {
                continue;
            }

            for (int i = 0; i < ATTRIBUTE_COUNT; ++i) {
                int field = Math.toIntExact(value.getLong(i));
                fieldProto.field.Builder fieldBuilder = fieldProto.field.newBuilder();
                builders.get(i).addFieldList(fieldBuilder.setDelta(key).setFieldValue(field).build());
            }
        }

        // add the compressed data to database
        try {
            Transaction tr = db.createTransaction();
            for (int i = 0; i < ATTRIBUTE_COUNT; ++i) {
                Tuple keyTuple = new Tuple();
                // key: (measurement, tag set, one field, start timestamp, entry count)
                // TODO: should be more generic
                keyTuple = keyTuple.add(measurement);
                keyTuple = keyTuple.add(tags.get(0).getKey());
                keyTuple = keyTuple.add(carId);
                keyTuple = keyTuple.add(fields.get(i).getKey());
                keyTuple = keyTuple.add(startTimestamp);
                keyTuple = keyTuple.add(data.size());
                byte[] key = keyTuple.pack();

                System.out.println("the flushed key: " + keyTuple);
                // value: the packed data
                byte[] value = builders.get(i).build().toByteArray();
                fdbTool.add(tr, key, value);
            }
            tr.commit();
            tr.close();
        } catch  (FDBException e) {
            e.printStackTrace();
        }
    }
}
