package org.sjtu.se.ipads.fdbserver.sqlparser.planner;


import org.apache.calcite.tools.*;

/**
 * {@link Frameworks Frameworks} is the second entry point of calcite after JDBC
 */
public class CalciteFramework {
    public static FrameworkConfig getDefaultConfig(){
        return Frameworks.newConfigBuilder().build();
    }
}
