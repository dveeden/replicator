package com.booking.replication.binlog;

/**
 * Created by bosko on 5/22/17.
 */
public class RawBinlogEvent_Xid extends RawBinlogEvent {
    public RawBinlogEvent_Xid(Object event) throws Exception {
        super(event);
    }
}
