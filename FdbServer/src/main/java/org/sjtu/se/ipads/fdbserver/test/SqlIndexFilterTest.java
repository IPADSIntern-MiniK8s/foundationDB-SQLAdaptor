//package org.sjtu.se.ipads.fdbserver.test;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import com.alibaba.fastjson.serializer.SerializerFeature;
//import com.apple.foundationdb.Database;
//import com.apple.foundationdb.FDB;
//import com.apple.foundationdb.subspace.Subspace;
//import com.apple.foundationdb.tuple.Tuple;
//import org.apache.calcite.sql.parser.SqlParseException;
//import org.apache.calcite.util.Sources;
//import org.apache.commons.io.FileUtils;
//import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;
//import org.sjtu.se.ipads.fdbserver.service.UploadService;
//import org.sjtu.se.ipads.fdbserver.sqlparser.SqlIndexFilter;
//import org.sjtu.se.ipads.fdbserver.utils.index.IndexManager;
//import org.sjtu.se.ipads.fdbserver.utils.index.MetaDataManager;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Map;
//
//@SpringBootApplication
//public class SqlIndexFilterTest {
//    public static void main(String[] args) {
//        try {
//            String indexPath = Sources.of(DemoAdaptorTest.class.getResource("/index.json"))
//                    .file().getAbsolutePath();
//            String metaDataPath = Sources.of(DemoAdaptorTest.class.getResource("/model.json"))
//                    .file().getAbsolutePath();
//
//            IndexManager indexManager = new IndexManager(indexPath);
//            MetaDataManager metaDataManager = new MetaDataManager(metaDataPath);
//
//            FDB fdb = FDB.selectAPIVersion(710);
//            Database db = fdb.open();
//            db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
//            db.options().setTransactionRetryLimit(100);
//
//            FdbTool fdbTool = new FdbTool();
//
//            // prepare input
//            // clean before
//            db.run(tx -> {
//                final byte[] st = new Subspace(new byte[]{(byte) 0x00}).getKey();
//                final byte[] en = new Subspace(new byte[]{(byte) 0xFF}).getKey();
//                tx.clear(st, en);
//                return null;
//            });
//
//            String filePath = Sources.of(DemoAdaptorTest.class.getResource("/batch_input.json"))
//                    .file().getAbsolutePath();
//            File file = new File(filePath);
//            String content = FileUtils.readFileToString(file);
//            JSONArray array = JSON.parseArray(content);
//            int arrSize = array.size();
//            UploadService uploadService = new UploadService();
//            for (int i = 0; i < arrSize; ++i) {
//                JSONObject jsonObject = array.getJSONObject(i);
//                boolean ret = uploadService.save(jsonObject, i + 1);
//                System.out.println("save entry result: " + ret);
//            }
//
////            String sql = "Select * from HEADER where MESSAGE_ID = 1";
////            boolean ret = sqlIndexFilter.preExecution(sql);
////            assert ret == false;
//            List<String> sqls = Arrays.asList(
////                    "Select * from HEADER where CAR_ID = 1",
////                    "Select * from HEADER where TIME_STAMP > 2277166961600969300 and TIME_STAMP <= 2277166961600969400",
//                    "Select * from HEADER where CAR_ID > 1 and MESSAGE_ID < 10",
//                    "Select TIME_STAMP,CAR_ID FROM HEADER where CAR_ID = 1",
//                    "Select * from GEOMETRY where X = 10344460");
//
//            for (String sql : sqls) {
//                SqlIndexFilter sqlIndexFilter = new SqlIndexFilter(indexManager, metaDataManager);
//                boolean ret = sqlIndexFilter.preExecution(sql);
//                System.out.println(sql + " preExecution result: " + ret);
//                if (ret) {
//                    System.out.println("-------------------------  " +
//                            "start sql"
//                            + "  -------------------------  ");
//                    byte[] key = Tuple.from("i_0", 1, 1).pack();
//                    byte[] res = fdbTool.query(db, key);
//                    assert Tuple.fromBytes(res).getLong(0) == 1;
//                    List<byte[]> queryRet = fdbTool.queryRange(db, "i_0", 1, 2);
//                    System.out.println(queryRet.size());
//                    List<Map<String, Object>> result = sqlIndexFilter.getQueryResult(db, fdbTool);
//                    String pretty = JSON.toJSONString(result,
//                            SerializerFeature.PrettyFormat,
//                            SerializerFeature.WriteMapNullValue,
//                            SerializerFeature.WriteDateUseDateFormat);
//                    System.out.println(pretty);
//                    System.out.println("-------------------------  " +
//                            "end sql"
//                            + "  -------------------------  ");
//                }
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (SqlParseException e) {
//            e.printStackTrace();
//        }
//    }
//}
