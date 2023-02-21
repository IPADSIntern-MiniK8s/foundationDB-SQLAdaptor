package org.sjtu.se.ipads.fdbserver.tsdb.log;

import lombok.Getter;

@Getter
public enum LogType {
    INSERT('0'),
    DELETE('1');

    private final char type;

    LogType(char type) {
        this.type = type;
    }
}
