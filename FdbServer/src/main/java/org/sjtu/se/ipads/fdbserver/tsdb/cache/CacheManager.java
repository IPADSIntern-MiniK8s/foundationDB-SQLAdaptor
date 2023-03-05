//package org.sjtu.se.ipads.fdbserver.tsdb.cache;
//
//
//import com.alibaba.fastjson.JSONObject;
//import com.apple.foundationdb.tuple.Tuple;
//import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
//import org.apache.commons.lang3.SerializationUtils;
//
//import java.io.ByteArrayInputStream;
//import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
//import java.io.Serializable;
//import java.util.*;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//import static java.lang.Math.max;
//
//
///**
// * @note don't store img in cache
// */
//public class CacheManager {
//    private static long startTimeStamp;
//    private static long halfHour = 120 * 60;
//    private static int lengthUpperbound = 5;  // TODO: the bound maybe need more discussion
//    private int clientCount;
//    private List<Map<Integer, Tuple>> storage;   // one storage for one car
//    private int maxLength;    // the max length of storage
//
//    private ExecutorService service;
//
//
//    /**
//     * deep copy
//     * @param obj
//     * @param <T>
//     * @return
//     */
//    private <T extends Serializable> T clone(T obj) {
//        T cloneObj = null;
//        try {
//            ByteOutputStream bos = new ByteOutputStream();
//            ObjectOutputStream oos = new ObjectOutputStream(bos);
//            oos.writeObject(obj);
//            oos.close();
//            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
//            ObjectInputStream ois = new ObjectInputStream(bis);
//            cloneObj = (T) ois.readObject();
//            ois.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return cloneObj;
//    }
//
//
//    // TODO: use redis in the next version
//    private List<Map<Integer, Tuple>> deepCopy() {
//        List<Map<Integer, Tuple>> storageCopy = new ArrayList<>();
//        for (Map<Integer, Tuple> elem : storage) {
//            Map<Integer, Tuple> newMap = new LinkedHashMap<>();
//            for (Map.Entry<Integer, Tuple> entry : elem.entrySet()) {
//                Tuple tuple = new Tuple();
//                tuple = tuple.addAll(entry.getValue());
//                newMap.put(entry.getKey(), tuple);
//            }
//            storageCopy.add(newMap);
//        }
//        return storageCopy;
//    }
//
//
//    public CacheManager(int client) {
//        clientCount = client;
//        Date date = new Date();
//        long dateVal = date.getTime() / 1000;   // erase the millisecond part
//        startTimeStamp = dateVal - halfHour;
//        storage = new ArrayList<>();
//        for (int i = 0; i < client; ++i) {
//            storage.add(new LinkedHashMap<Integer, Tuple>());
//        }
//        maxLength = 0;
//        service = Executors.newFixedThreadPool(client * 5);
//    }
//
//
//    public void updateCache() {
//        // update start time
//        Date date = new Date();
//        long dateVal = date.getTime() / 1000;   // erase the millisecond part
//        startTimeStamp = dateVal - halfHour;
//        storage = new ArrayList<>(clientCount);
//        for (int i = 0; i < clientCount; ++i) {
//            storage.add(new LinkedHashMap<Integer, Tuple>());
//        }
//    }
//
//
//    public Map<Integer, Tuple> getStorage(int id) {
//        return storage.get(id);
//    }
//
//
//    public List<Map<Integer, Tuple>> getAllStorage() {
//        return storage;
//    }
//
//
//    public boolean insertToCache(String timestamp, Tuple tuple, int id, String img) {
//        long rawTimestamp = Long.valueOf(timestamp);
//        System.out.println("result" + (rawTimestamp < startTimeStamp));
//        if (rawTimestamp < startTimeStamp) {
//            return false;
//        }
//        int delta = (int) (rawTimestamp - startTimeStamp);
//        storage.get(id - 1).put(delta, tuple);
//
//        // create a new thread for img storage
//        ImgStore imgStore = new ImgStore(id, rawTimestamp, img);
//        service.execute(imgStore);
//        maxLength = max(maxLength, storage.get(id - 1).size());
//        return true;
//    }
//
//
//    public void triggerFlush() {
//        if (maxLength <= lengthUpperbound) {
//            return;
//        }
//        List<Map<Integer, Tuple>> storageCopy = deepCopy();
//        for (int i = 0; i < clientCount; ++i) {
//            CacheFlush cacheFlushThread = new CacheFlush(i, startTimeStamp, storageCopy.get(i));
//            service.execute(cacheFlushThread);
//        }
//        updateCache();
//    }
//
//}
