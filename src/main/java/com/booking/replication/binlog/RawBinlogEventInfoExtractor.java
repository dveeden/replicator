package com.booking.replication.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.common.util.MySQLConstants;

/**
 * Generic class for working with binlog events from different parser providers
 */
public class RawBinlogEventInfoExtractor {

    private final int     BINLOG_PARSER_PROVIDER;
    private BinlogEventV4 binlogEventV4;
    private Event         binlogConnectorEvent;
    private long          timestampOfReceipt;

    public RawBinlogEventInfoExtractor(Object event) throws Exception {

        // timeOfReceipt in OpenReplicator is set as:
        //
        //       header.setTimestampOfReceipt(System.currentTimeMillis());
        //
        // Since this field does not exists in BinlogConnector and its not
        // really binlog related, but it is time when the parser received the
        // event, we can use here System.currentTimeMillis() and it will
        // represent the time when the producer created this object.
        this.timestampOfReceipt = System.currentTimeMillis();

        if (event instanceof BinlogEventV4) {
            BINLOG_PARSER_PROVIDER = BinlogEventParserProviderCode.OR;

            // this can be done in a nicer way by wraping the twp respective types
            // into wraper objects which have an additional property 'ON/OFF' so
            // we avoid the null assignmetns. Since we use th
            binlogEventV4 = (BinlogEventV4) event;
            binlogConnectorEvent = null;

        }
        else if (event instanceof Event) {
            BINLOG_PARSER_PROVIDER = BinlogEventParserProviderCode.SHYIKO;
            binlogEventV4 = null;
            binlogConnectorEvent = (Event) event;
        }
        else {
            throw new Exception("Unsupported parser!");
        }
    }

    public boolean hasHeader() {
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader() != null);
        }
        else {
            return (binlogConnectorEvent.getHeader() != null);
        }
    }

    public long getTimestampOfReceipt() {
       return this.getTimestampOfReceipt();
    }

    public long getTimestamp() {
        if (binlogEventV4 != null) {
            return binlogEventV4.getHeader().getTimestamp();
        }
        else {
            return binlogConnectorEvent.getHeader().getTimestamp();
        }
    }

    public boolean isQuery() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader().getEventType() == MySQLConstants.QUERY_EVENT);
        }
        else {
            return (binlogConnectorEvent.getHeader().getEventType() == EventType.QUERY);
        }
    }

    public boolean isTableMap() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader().getEventType() == MySQLConstants.TABLE_MAP_EVENT);
        }
        else {
            return (binlogConnectorEvent.getHeader().getEventType() == EventType.TABLE_MAP);
        }
    }

    public boolean isUpdateRows() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (
                (binlogEventV4.getHeader().getEventType() == MySQLConstants.UPDATE_ROWS_EVENT_V2)
                ||
                (binlogEventV4.getHeader().getEventType() == MySQLConstants.UPDATE_ROWS_EVENT)
            );
        }
        else {
            return EventType.isUpdate(binlogConnectorEvent.getHeader().getEventType());
        }
    }

    public boolean isWriteRows() {
        if (binlogEventV4 != null) {
            return (
                (binlogEventV4.getHeader().getEventType() == MySQLConstants.WRITE_ROWS_EVENT_V2)
                ||
                (binlogEventV4.getHeader().getEventType() == MySQLConstants.WRITE_ROWS_EVENT)
            );
        }
        else {
            return EventType.isWrite(binlogConnectorEvent.getHeader().getEventType());
        }
    }

    public boolean isDeleteRows() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (
                (binlogEventV4.getHeader().getEventType() == MySQLConstants.DELETE_ROWS_EVENT_V2)
                ||
                (binlogEventV4.getHeader().getEventType() == MySQLConstants.DELETE_ROWS_EVENT)
            );
        }
        else {
            return EventType.isDelete(binlogConnectorEvent.getHeader().getEventType());
        }
    }

    public boolean  isXid() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader().getEventType() == MySQLConstants.XID_EVENT);
        }
        else {
            return (binlogConnectorEvent.getHeader().getEventType() == EventType.XID);
        }
    }

    public boolean isFormatDescription() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader().getEventType() == MySQLConstants.FORMAT_DESCRIPTION_EVENT);
        }
        else {
            return (binlogConnectorEvent.getHeader().getEventType() == EventType.FORMAT_DESCRIPTION);
        }
    }

    public boolean isRotate() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader().getEventType() == MySQLConstants.ROTATE_EVENT);
        }
        else {
            return (binlogConnectorEvent.getHeader().getEventType() == EventType.ROTATE);
        }
    }

    public boolean isStop() {
        // All constants from OR are Enums in BinlogConnector so need to check for both
        if (binlogEventV4 != null) {
            return (binlogEventV4.getHeader().getEventType() == MySQLConstants.STOP_EVENT);
        }
        else {
            return (binlogConnectorEvent.getHeader().getEventType() == EventType.STOP);
        }
    }
}
