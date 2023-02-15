package org.sjtu.se.ipads.fdbserver.sqlparser.planner;


import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;


public class CalciteSqlNode {

    /**
     * Validate the SQl Node against the schema with a planner object
     *
     * @return the validated node
     */
    public static SqlNode validate(Planner planner, SqlNode sqlNode) throws ValidationException {
        return CalciteSqlValidation.validateFromPlanner(planner, sqlNode);
    }


    /**
     * From SqlToRelRoot
     *
     * @param planner - A planner utility. See { CalcitePlanner#getPlannerFromFrameworkConfig(FrameworkConfig)}
     * @return a relRoot
     */
    public static RelRoot fromSqlNodeToRelRootViaPlanner(Planner planner, SqlNode sqlNode) {
        try {
            return planner.rel(sqlNode);
        } catch (RelConversionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param sql
     * @return a SqlNode from a Sql
     */
    public static SqlNode fromSqlToSqlNode(String sql) {
        return CalciteSql.fromSqlToSqlNode(sql);
    }
}
