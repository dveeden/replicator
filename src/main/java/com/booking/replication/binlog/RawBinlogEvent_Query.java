package com.booking.replication.binlog;

import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.google.code.or.binlog.impl.event.QueryEvent;

/**
 * Created by bosko on 5/22/17.
 */
public class RawBinlogEvent_Query extends RawBinlogEvent {
    public RawBinlogEvent_Query(Object event) throws Exception {
        super(event);
    }

    public String getSql() {
        if (USING_DEPRECATED_PARSER) {
            return ((QueryEvent) this.getBinlogEventV4()).getSql().toString();
        }
        else {
            return ((QueryEventData) this.getBinlogConnectorEvent().getData()).getSql();
        }
    }

    public String getDatabaseName() {
        if (USING_DEPRECATED_PARSER) {
            return ((QueryEvent) this.getBinlogEventV4()).getDatabaseName().toString();
        }
        else {
            return ((QueryEventData) this.getBinlogConnectorEvent().getData()).getDatabase();
        }
    }
}
