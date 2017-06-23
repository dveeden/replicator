package com.booking.replication.binlog.common.cell;

import com.booking.replication.binlog.common.Cell;

/**
 * Extracted from: https://github.com/whitesock/open-replicator/blob/master/src/main/java/com/google/code/or/common/glossary/column/TimestampColumn.java
 */
public class TimestampCell implements Cell {

    private final java.sql.Timestamp value;

    public TimestampCell(java.sql.Timestamp value) {
        this.value = value;
    }

    @Override
    public Object getValue() {
        return value;
    }
}
