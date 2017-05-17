package com.booking.replication.binlog;

/**
 * Created by bosko on 5/17/17.
 */
public enum RawEventType {

    QUERY_EVENT,

    TABLE_MAP_EVENT,

    UPDATE_ROWS_EVENT,

    UPDATE_ROWS_EVENT_V2,

    WRITE_ROWS_EVENT,

    WRITE_ROWS_EVENT_V2,

    DELETE_ROWS_EVENT,

    DELETE_ROWS_EVENT_V2,

    XID_EVENT,

    FORMAT_DESCRIPTION_EVENT,

    ROTATE_EVENT,

    STOP_EVENT

}
