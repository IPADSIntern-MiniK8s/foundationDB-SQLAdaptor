package org.sjtu.se.ipads.fdbserver.test;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.sjtu.se.ipads.fdbserver.sqlparser.planner.CalciteFramework;
import org.sjtu.se.ipads.fdbserver.sqlparser.planner.CalcitePlanner;
import org.sjtu.se.ipads.fdbserver.sqlparser.planner.CalciteSqlSelect;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SqlParseTest {
    public static void main(String[] args) throws SqlParseException {
        FrameworkConfig config = CalciteFramework.getDefaultConfig();
        Planner planner = CalcitePlanner.getPlannerFromFrameworkConfig(config);
//        SqlNode sqlNode = planner.parse("select deptno, count(1) from emp where SAL between 1000 and 2000 group by deptno");
//        SqlNode sqlNode = planner.parse("Select name1, name2 from emp where sal > 2000 AND sal < (3000 - (3.0 * 5))");

        SqlNode sqlNode = planner.parse("Select * from HEADER where CAR_ID > 1 and MESSAGE_ID < 10");
        CalciteSqlSelect.printInfo(sqlNode);
    }
}
