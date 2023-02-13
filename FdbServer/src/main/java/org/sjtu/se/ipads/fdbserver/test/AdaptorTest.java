package org.sjtu.se.ipads.fdbserver.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.calcite.util.Sources;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.sjtu.se.ipads.fdbserver.adapter.CalciteUtil;
import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;


@SpringBootApplication
public class AdaptorTest {

    public static void main(String[] args) {
        String filePath =  Sources.of(AdaptorTest.class.getResource("/model.json"))
                .file().getAbsolutePath();
        String model;

        Map<String, Integer> TABLE_MAPS = new HashMap<>(2);
        TABLE_MAPS.put("TEST1", 1);
        TABLE_MAPS.put("TEST2", 2);

        // insert data
        FDB fdb = FDB.selectAPIVersion(710);
        Database db = fdb.open();
        db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
        db.options().setTransactionRetryLimit(100);
        FdbTool fdbTool = new FdbTool();

        String table_name = "TEST1";
        for (long i = 0; i < 10; ++i) {
            byte[] key = Tuple.from(table_name,  Long.toString(i)).pack();
            fdbTool.add(db, key, Tuple.from(Long.toString(i), "112233").pack());
        }

        table_name = "TEST2";
        for (long i = 0; i < 10; ++i) {
            byte[] key = Tuple.from(table_name,  Long.toString(i)).pack();
            fdbTool.add(db, key, Tuple.from(Long.toString(i), "445566").pack());
        }

        // readModelByJson
        String strResult = null;
        try {
            ObjectMapper objMapper = new ObjectMapper();
            objMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
                    .configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
                    .configure(JsonParser.Feature.ALLOW_COMMENTS, true);
            File file = new File(filePath);
            if (file.exists()) {
                JsonNode rootNode = objMapper.readTree(file);
                strResult = rootNode.toString();
            }
        } catch (Exception ignored) {
        }
        model = strResult;

        // testBySql
        Connection connection = null;
        Statement statement = null;
        try{
            connection = CalciteUtil.getConnect("/model.json");
            statement = connection.createStatement();
            String[] strArray = {
                    "select * from TEST1",
                    "select * from TEST1 as t1 left join TEST2 as t2 on t1.MESSAGE_ID=t2.MESSAGE_ID",
                    "select * from TEST1 where MESSAGE_ID=1",
                    "select * from TEST2 where MESSAGE_ID > 2 and MESSAGE_ID < 7"
            };

            for (String sql : strArray) {
                ResultSet resultSet = statement.executeQuery(sql);

                System.out.println("-------------------------  " +
                        "start sql"
                        + "  -------------------------  ");
                String pretty = JSON.toJSONString(CalciteUtil.getData(resultSet),
                        SerializerFeature.PrettyFormat,
                        SerializerFeature.WriteMapNullValue,
                        SerializerFeature.WriteDateUseDateFormat);
                System.out.println(pretty);
                System.out.println("-------------------------  " +
                        "end sql"
                        + "  -------------------------  ");
            }


        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                statement.close();
                connection.close();
            }catch (SQLException e){
                e.printStackTrace();
            }
        }

    }
}
