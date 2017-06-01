package com.booking.replication.binlog;

/**
 * Created by bosko on 6/1/17.
 */
public class RawBinlogEvent_Update extends RawBinlogEvent_Rows {
    public RawBinlogEvent_Update(Object event) throws Exception {
        super(event);
    }
}
