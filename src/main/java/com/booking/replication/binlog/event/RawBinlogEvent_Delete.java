package com.booking.replication.binlog.event;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;

import java.util.BitSet;

/**
 * Created by bosko on 6/1/17.
 */
public class RawBinlogEvent_Delete extends RawBinlogEvent_Rows {

    public RawBinlogEvent_Delete(Object event) throws Exception {
        super(event);
    }

    public int getColumnCount() {
        if (this.binlogEventV4 != null) {
            return ((WriteRowsEvent) binlogEventV4).getColumnCount().intValue();
        }
        else {
            BitSet includedColumns = ((DeleteRowsEventData) binlogConnectorEvent.getData()).getIncludedColumns();
            return includedColumns.cardinality();
        }
    }

    public long getTableId() {
        if (this.binlogEventV4 != null) {
            return ((WriteRowsEvent) binlogEventV4).getTableId();
        }
        else {
            return ((WriteRowsEventData) binlogConnectorEvent.getData()).getTableId();
        }
    }
}
