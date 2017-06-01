package com.booking.replication.binlog;

/**
 * Created by bosko on 6/1/17.
 */
public class RawBinlogEvent_Write extends RawBinlogEvent_Rows {
    public RawBinlogEvent_Write(Object event) throws Exception {
        super(event);
    }
}
