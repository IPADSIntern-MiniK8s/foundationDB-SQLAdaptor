//package org.sjtu.se.ipads.fdbserver.test;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import org.apache.calcite.util.Sources;
//import org.apache.commons.io.FileUtils;
//import org.sjtu.se.ipads.fdbserver.service.UploadService;
//import org.sjtu.se.ipads.fdbserver.tsdb.log.LogManager;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//
//import java.io.File;
//import java.io.IOException;
//
//
//@SpringBootApplication
//public class LogTest {
//    // test log write and restore
//    public static void main(String[] args) throws IOException {
//        LogManager logManager = new LogManager(5, true);
//
//        String filePath = Sources.of(DemoAdaptorTest.class.getResource("/batch_input.json"))
//                .file().getAbsolutePath();
//
//        File file = new File(filePath);
//        String content = FileUtils.readFileToString(file);
//        JSONArray array = JSON.parseArray(content);
//        int arrSize = array.size();
//        UploadService uploadService = new UploadService();
//        for (int i = 0; i < arrSize; ++i) {
//            JSONObject jsonObject = array.getJSONObject(i);
//        }
//    }
//}
