package com.booking.replication.binlog.event;

/**
 * Created by bosko on 5/22/17.
 */
public class RawBinlogEventXid extends RawBinlogEvent {
    public RawBinlogEventXid(Object event) throws Exception {
        super(event);
    }
}
