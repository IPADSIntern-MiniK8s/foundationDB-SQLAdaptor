package org.sjtu.se.ipads.fdbserver.sqlparser.planner;

import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.*;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.ValidationException;

public class CalciteSqlValidation {


    /**
     *
     * @param planner
     * @param sqlNode
     * @return the validated node
     */
    static SqlNode validateFromPlanner(Planner planner, SqlNode sqlNode) {
        try {
            return planner.validate(sqlNode);
        } catch (ValidationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Build a validator with:
     *   * a {@link SqlConformanceEnum#DEFAULT}
     *   * and
     * @param sqlNode
     * @return the validated node
     */
    static SqlNode validateFromUtilValidatorPlanner(Prepare.CatalogReader catalogReader, SqlNode sqlNode) {

        SqlStdOperatorTable operatorTable = SqlStdOperatorTable.instance();
        SqlValidatorWithHints sqlValidator = SqlValidatorUtil.newValidator(
                operatorTable,
                catalogReader,
                catalogReader.getTypeFactory(),
                SqlConformanceEnum.DEFAULT
        );
        return sqlValidator.validate(sqlNode);

    }
}
