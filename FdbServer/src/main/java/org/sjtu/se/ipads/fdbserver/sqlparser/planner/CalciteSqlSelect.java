package org.sjtu.se.ipads.fdbserver.sqlparser.planner;

import org.apache.calcite.sql.*;

public class CalciteSqlSelect {

    static public void printInfo(SqlNode sqlNode) {
        if (sqlNode instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) sqlNode;

            SqlBasicCall where = (SqlBasicCall) select.getWhere();
            SqlNodeList selectlist = select.getSelectList();
            SqlNode[] operands = where.getOperands();
            SqlNode from = select.getFrom();
            SqlKind kind = from.getKind();

            for (SqlNode node : operands) {
                System.out.println(node);
            }
            SqlOperator operator = where.getOperator();

            System.out.println("where elem is: " + where.getOperator());
            System.out.println("where ");
            System.out.println("Select list is: " + select.getSelectList());
            System.out.println("From clause is: " + select.getFrom());
            System.out.println("Where clause is: " + select.getWhere());
            System.out.println("Group clause is: " + select.getGroup());

        } else {
            throw new RuntimeException("This is not a select statement. The class of this SqlNode is " + sqlNode.getClass().toString());
        }

    }
}
