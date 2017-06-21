package com.booking.replication.binlog.common.column;

import com.booking.replication.binlog.common.ExtractedColumn;
import com.google.code.or.common.util.MySQLConstants;

/**
 * Created by bosko on 6/21/17.
 *
 * Just a chunk of bytes that will later be interpreted
 * according to its data type.
 *
 */
public class AnyColumn implements ExtractedColumn {

    private final byte[] value;

    public AnyColumn(byte[] value) {
        this.value = value;
    }

    @Override
    public Object getValue() {
        return this.value;
    }
}
