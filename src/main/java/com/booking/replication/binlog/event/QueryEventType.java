package com.booking.replication.binlog.event;

/**
 * Created by edmitriev on 8/1/17.
 */
public enum QueryEventType {
    BEGIN,
    COMMIT,
    DDLTABLE,
    DDLTEMPORARYTABLE,
    DDLVIEW,
    PSEUDOGTID,
    ANALYZE,
    UNKNOWN;
    QueryEventType() {
    }
}
