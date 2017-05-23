package com.booking.replication.binlog;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.google.code.or.binlog.impl.event.TableMapEvent;

/**
 * Created by bosko on 5/22/17.
 */
public class RawBinlogEvent_TableMap extends RawBinlogEvent {

    public RawBinlogEvent_TableMap(Object event) throws Exception {
        super(event);
    }

    public long getTableId() {
        if (this.USING_DEPRECATED_PARSER) {
            return  ((TableMapEvent) this.getBinlogEventV4()).getTableId();
        }
        else {
            return ((TableMapEventData) this.getBinlogConnectorEvent().getData()).getTableId();
        }
    }

    public String getTableName() {
        if (this.USING_DEPRECATED_PARSER) {
            return  ((TableMapEvent) this.getBinlogEventV4()).getTableName().toString();
        }
        else {
            return ((TableMapEventData) this.getBinlogConnectorEvent().getData()).getTable();
        }
    }

    public String getDatabaseName() {
        if (this.USING_DEPRECATED_PARSER) {
            return  ((TableMapEvent) this.getBinlogEventV4()).getDatabaseName().toString();
        }
        else {
            return ((TableMapEventData) this.getBinlogConnectorEvent().getData()).getDatabase();
        }
    }
}
