package org.sjtu.se.ipads.fdbserver.sqlparser;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Planner;
import org.sjtu.se.ipads.fdbserver.basicop.FdbTool;
import org.sjtu.se.ipads.fdbserver.sqlparser.planner.CalciteFramework;
import org.sjtu.se.ipads.fdbserver.sqlparser.planner.CalcitePlanner;
import org.sjtu.se.ipads.fdbserver.utils.index.IndexManager;
import org.sjtu.se.ipads.fdbserver.utils.index.MetaDataManager;

import java.util.*;

public class SqlIndexFilter {
    private IndexManager indexmanager;
    private MetaDataManager metaDataManager;
    private Planner planner;
    private SqlNode sqlnode;
    private SqlBasicCall where;
    private SqlNode orderBy;
    private List<String> selects;
    private String from;
    private List<SqlKind> operators;
    private List<Map.Entry<String, String>> conditionMap;


    SqlIndexFilter(IndexManager indexManager, MetaDataManager metaDataManager) {
        this.indexmanager = indexManager;
        this.metaDataManager = metaDataManager;
        FrameworkConfig config = CalciteFramework.getDefaultConfig();
        this.planner = CalcitePlanner.getPlannerFromFrameworkConfig(config);
        selects = new ArrayList<>();
        operators = new ArrayList<>();
        conditionMap = new ArrayList<>();
    }

    private void initVariable() {
        sqlnode = null;
        where = null;
        orderBy = null;
        selects.clear();
        from = "";
        operators.clear();
        conditionMap.clear();
    }

    /**
     * check whether it is a valid exp in 'where' clause
     * the valid format: identifier op numericLiteral
     * the valid op inside: >, >=, =, <= , <
     * @param exp
     * @return
     */
    private boolean checkValidExp(SqlBasicCall exp) {
        if (exp.getOperator() == null) {
            return false;
        }
        SqlOperator operator = exp.getOperator();
        if (operator.getKind().equals(SqlKind.GREATER_THAN) || operator.getKind().equals(SqlKind.GREATER_THAN_OR_EQUAL)
            || operator.getKind().equals(SqlKind.EQUALS) || operator.getKind().equals(SqlKind.LESS_THAN) ||
            operator.getKind().equals(SqlKind.LESS_THAN_OR_EQUAL)) {
            SqlNode[] operands = exp.getOperands();
            if (operands.length != 2 || !(operands[0] instanceof SqlIdentifier) || !(operands[1] instanceof SqlNumericLiteral)) {
                return false;
            } else {
                conditionMap.add(new AbstractMap.SimpleEntry<>(operands[0].toString(), operands[1].toString()));
                operators.add(operator.getKind());
                return true;
            }
        } else {
            return false;
        }
    }

    /**
     * in order to use index: Optimize the select sql where there is an index in the `where` condition
     * @return can use index or not
     */
    public boolean preExecution(String sql) throws SqlParseException {
        initVariable();
        if (!(sqlnode instanceof SqlSelect)) {
            return false;
        }

        SqlSelect sqlSelect = (SqlSelect) sqlnode;

        // if too complex, escape
        if (sqlSelect.getGroup() != null || sqlSelect.getFetch() != null || sqlSelect.getHaving() != null || sqlSelect.getWindowList() != null) {
            return false;
        }

        // determine the name of the table to check
        if (sqlSelect.getFrom() == null || !(sqlSelect.getFrom() instanceof SqlIdentifier)) {
            return false;
        } else {
            from = sqlSelect.getFrom().toString();
        }

        // if not have `where`, absolutely can't use index
        if (sqlSelect.getWhere() == null) {
            return false;
        } else {
            where = (SqlBasicCall) sqlSelect.getWhere();
            SqlOperator operator = where.getOperator();
            if (operator.getKind().equals(SqlKind.AND) || operator.getKind().equals(SqlKind.OR) ) {
                if (where.getOperands().length != 2 || !(where.getOperands()[0] instanceof SqlBasicCall) || !(where.getOperands()[1] instanceof SqlBasicCall)) {
                    return false;
                } else {
                    SqlBasicCall left = (SqlBasicCall) where.getOperands()[0];
                    SqlBasicCall right = (SqlBasicCall) where.getOperands()[1];
                    if (!checkValidExp(left) || !checkValidExp(right)) {
                        return false;
                    }

                    // check two conditions' cooperate
                    if (conditionMap.size() == 2 && conditionMap.get(0).getKey() == conditionMap.get(1).getKey()) {
                        if (operators.get(1).equals(SqlKind.GREATER_THAN_OR_EQUAL) || operators.get(2).equals(SqlKind.GREATER_THAN)) {
                            Collections.swap(conditionMap, 0, 1);
                            Collections.swap(operators, 0, 1);
                        }
                        if (!(operators.get(0).equals(SqlKind.GREATER_THAN) || operators.get(0).equals(SqlKind.GREATER_THAN_OR_EQUAL))
                                || !(operators.get(1).equals(SqlKind.LESS_THAN) || operators.get(1).equals(SqlKind.LESS_THAN_OR_EQUAL))) {
                            return false;
                        }
                    }

                    // check whether contain index
                    int isIndexLeft = indexmanager.hasIndex(from, conditionMap.get(0).getKey());
                    int isIndexRight = indexmanager.hasIndex(from, conditionMap.get(1).getKey());

                    if (operator.getKind().equals(SqlKind.AND) && isIndexLeft != -1) {
                        return false;
                    } else if (operator.getKind().equals(SqlKind.OR) && (isIndexLeft != -1 || isIndexRight != -1)) {
                        return false;
                    }

                    operators.add(operator.getKind());
                }
            } else if (!checkValidExp(where) || indexmanager.hasIndex(from, conditionMap.get(0).getKey()) == -1) {
                return false;
            }
        }


        if (sqlSelect.getSelectList() == null) {
            return false;
        } else {
            SqlNodeList selectList = sqlSelect.getSelectList();
            List<SqlNode> list = selectList.getList();
            for (SqlNode node : list) {
                if (!(node instanceof SqlIdentifier)) {
                    return false;
                } else {
                    selects.add(node.toString());
                }
            }
        }

        // check order by
        orderBy = sqlSelect.getOrderList();
        return true;
    }

    /**
     * get the index bound for query range
     * @param i
     * @return leftBound, RightBound
     */
    private List<String> getBound(int i) {
        String leftBound = "", rightBound = "";
        int rawValue = Integer.parseInt(conditionMap.get(i).getValue());
        switch (operators.get(0)) {
            case EQUALS:
                leftBound = String.valueOf(rawValue);
                rightBound = String.valueOf(rawValue + 1);
                break;
            case GREATER_THAN:
                leftBound = String.valueOf(rawValue - 1);
                rightBound = "XXX";
                break;
            case GREATER_THAN_OR_EQUAL:
                leftBound = String.valueOf(rawValue);
                rightBound = "XXX";
                break;
            case LESS_THAN:
                leftBound = "";
                rightBound = String.valueOf(rawValue);
                break;
            case LESS_THAN_OR_EQUAL:
                leftBound = "";
                rightBound = String.valueOf(rawValue + 1);
            default:
                break;
        }
        return Arrays.asList(leftBound, rightBound);
    }


    private Set<String> getPrimaryKeys(List<byte[]> rawData) {
        Set<String> result = new HashSet<>();
        for (byte[] elem : rawData) {
            result.add(Tuple.fromBytes(elem).getString(0));
        }
        return result;
    }


    /**
     * parse the raw data into something readable
     * @param rawData
     * @return
     */
    private Map<String, Object> decodeEntry(byte[] rawData) {
        List<String> entryNames = metaDataManager.getEntryNames(from);
        int size = Tuple.fromBytes(rawData).size();
        if (entryNames.size() != size) {
            return null;
        }
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < size; ++i) {
            result.put(entryNames.get(i), Tuple.fromBytes(rawData).get(i));
        }
        return result;
    }


    private List<Map<String, Object>> noIndexFilter(List<Map<String, Object>> preData, String attribute, SqlKind op, int targetVal) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> elem : preData) {
            int rawData = Integer.parseInt(elem.get(attribute).toString());
            switch (op) {
                case EQUALS:
                    if (rawData == targetVal) {
                        result.add(elem);
                    }
                    break;
                case GREATER_THAN:
                    if (rawData > targetVal) {
                        result.add(elem);
                    }
                    break;
                case GREATER_THAN_OR_EQUAL:
                    if (rawData >= targetVal) {
                        result.add(elem);
                    }
                    break;
                case LESS_THAN:
                    if (rawData < targetVal) {
                        result.add(elem);
                    }
                    break;
                case LESS_THAN_OR_EQUAL:
                    if (rawData <= targetVal) {
                        result.add(elem);
                    }
                    break;
                default:
                    break;
            }
        }
        return result;
    }


    List<Map<String, Object>> getQueryResult(Database db, FdbTool fdbTool) {
        // first select the index table

        // check the first condition
        List<String> ret = getBound(0);
        String leftBound = ret.get(0);
        String rightBound = ret.get(1);

        // get the index table
        int indexTableNumber = indexmanager.hasIndex(from, conditionMap.get(0).getKey());
        String indexTableName = "i_" + String.valueOf(indexTableNumber);
        // get the first condition filtered primary key
        List<byte[]> indexFirstQuery = fdbTool.queryRange(db, indexTableName, leftBound, rightBound);
        List<byte[]> indexSecondQuery = new ArrayList<>();
        boolean secondIndexed = true;

        // if has the second condition, check the second condition
        if (conditionMap.size() > 1) {
            ret = getBound(1);
            leftBound = ret.get(0);
            rightBound = ret.get(1);
            indexTableNumber = indexmanager.hasIndex(from, conditionMap.get(0).getKey());
            if (indexTableNumber != -1) {
                indexTableName = "i_" + String.valueOf(indexTableNumber);
                indexSecondQuery = fdbTool.queryRange(db, indexTableName, leftBound, rightBound);
            } else {
                secondIndexed = false;
            }
        }

        // merge all eligible entries and get the primary key
        Set<String> primaryKeys = getPrimaryKeys(indexFirstQuery);
        if (!indexSecondQuery.isEmpty()) {
            if (operators.get(2).equals(SqlKind.AND)) {
                primaryKeys.retainAll(getPrimaryKeys(indexSecondQuery));
            } else if (operators.get(2).equals(SqlKind.OR)) {
                primaryKeys.addAll(getPrimaryKeys(indexSecondQuery));
            }
        }


        List<Map<String, Object>> result = new ArrayList<>();
        for (String key : primaryKeys) {
            result.add(decodeEntry(fdbTool.query(db, Tuple.from(from, key).pack())));
        }

        // has the un-indexed filter
        if (secondIndexed == false) {
            String attribute = conditionMap.get(1).getKey();
            if (metaDataManager.hasEntry(from, attribute)) {
                int target = Integer.parseInt(conditionMap.get(1).getValue());
                result = noIndexFilter(result, attribute, operators.get(1), target);
            }
        }
        return result;
    }
}
