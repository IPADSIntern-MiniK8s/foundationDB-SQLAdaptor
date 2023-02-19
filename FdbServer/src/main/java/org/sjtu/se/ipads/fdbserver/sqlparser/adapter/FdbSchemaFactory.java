package org.sjtu.se.ipads.fdbserver.sqlparser.adapter;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Map;

/**
 * Factory that creates a {@link FdbSchema}.
 *
 * <p>Allows a custom schema to be included in a model.json file.
 * See <a href="http://calcite.apache.org/docs/file_adapter.html">File adapter</a>.
 */
public class FdbSchemaFactory implements SchemaFactory {
    public FdbSchemaFactory() {}

    @Override public Schema create(SchemaPlus schema, String name, Map<String, Object> operand) {
        List<Map<String, Object>> tables = (List) operand.get("tables");
        int database = Integer.parseInt(operand.get("database").toString());
        String flavorName = (String) operand.get("flavor");
        return new FdbSchema(database, tables, flavorName);
    }

}
