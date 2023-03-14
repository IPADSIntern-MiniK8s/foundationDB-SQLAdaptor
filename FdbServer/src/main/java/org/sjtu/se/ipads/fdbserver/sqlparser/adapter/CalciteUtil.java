package org.sjtu.se.ipads.fdbserver.sqlparser.adapter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.net.URL;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CalciteUtil {
    /**
     * 根据给定的 model.json 文件获取 Connection
     *
     * @param filePath
     * @return
     */
    public static Connection getConnect() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:calcite:model=./model.json");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 归集查询后的数据并注入到 List
     * @param resultSet
     * @return
     * @throws Exception
     */
    public static List<Map<String, Object>> getData(ResultSet resultSet) throws Exception {
        List<Map<String, Object>> list = Lists.newArrayList();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnSize = metaData.getColumnCount();

        while (resultSet.next()) {
            Map<String, Object> map = Maps.newLinkedHashMap();
            for (int i = 1; i < columnSize + 1; i++) {
                String label = metaData.getColumnLabel(i);
                switch (label){
                    case "X":
                    case "Y":
                    case "DIRECTION":
                    case "V_X":
                    case "V_Y":
                    case "V_R":
                        map.put(label, Double.valueOf(resultSet.getString(i))/1000);
                        break;
                    default:
                        map.put(label, resultSet.getString(i));
                }
            }
            list.add(map);
        }
        return list;
    }
}
