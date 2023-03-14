package org.sjtu.se.ipads.fdbserver.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;
import org.sjtu.se.ipads.fdbserver.sqlparser.adapter.CalciteUtil;
import org.sjtu.se.ipads.fdbserver.sqlparser.planner.CalciteFramework;
import org.sjtu.se.ipads.fdbserver.sqlparser.planner.CalcitePlanner;
import org.sjtu.se.ipads.fdbserver.sqlparser.planner.CalciteSqlSelect;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class QueryService {

    private FdbTool fdbTool;
    private Database db;
    private Planner planner;
    public QueryService(){
        FDB fdb = FDB.selectAPIVersion(710);
        db = fdb.open();
        db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
        db.options().setTransactionRetryLimit(100);

        fdbTool = new FdbTool();


        FrameworkConfig config = CalciteFramework.getDefaultConfig();
        planner = CalcitePlanner.getPlannerFromFrameworkConfig(config);
    }

    public String getDataByMessageID(int messageID){
        String res = "";
        List<String> tables = Arrays.asList("HEADER", "GEOMETRY", "IMAGE");
        try {
            for (String table : tables) {
                byte[] key = Tuple.from(table, messageID).pack();
                byte[] result = fdbTool.query(db, key);
                List<Object> entry = new ArrayList<>();
                int size = Tuple.fromBytes(result).size();
                for (int i = 1; i < size; ++i) {//first is message id
                    if(table == "GEOMETRY"){
                        entry.add(Double.valueOf(Tuple.fromBytes(result).get(i).toString())/1000);
                    }else{
                        entry.add(Tuple.fromBytes(result).get(i));
                    }
                }
                res += table+":"+ entry+"\n";
//            System.out.println(entry.toString());
            }
        } catch (Exception e) {
            return res;
        }
        return res;
    }

    public String queryBySQL(String sql){

        Connection connection = null;
        Statement statement = null;
        try{
            connection = CalciteUtil.getConnect();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            String pretty = JSON.toJSONString(CalciteUtil.getData(resultSet),
                    SerializerFeature.PrettyFormat,
                    SerializerFeature.WriteMapNullValue,
                    SerializerFeature.WriteDateUseDateFormat);
            return pretty;

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
        return "";
    }

}
