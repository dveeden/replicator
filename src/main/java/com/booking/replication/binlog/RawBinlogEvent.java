package com.booking.replication.binlog;

import com.github.shyiko.mysql.binlog.event.*;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.common.util.MySQLConstants;

/**
 * Generic class for working with binlog events from different parser providers
 */
public class RawBinlogEvent {

    private final int     BINLOG_PARSER_PROVIDER;
    private BinlogEventV4 binlogEventV4;
    private Event         binlogConnectorEvent;
    private long          timestampOfReceipt;
    private long          timestampOfBinlogEvent;
    public final boolean USING_DEPRECATED_PARSER;

    public RawBinlogEvent(Object event) throws Exception {

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
            binlogConnectorEvent = null;

            binlogEventV4 = (BinlogEventV4) event;
            timestampOfBinlogEvent = binlogEventV4.getHeader().getTimestamp();

            USING_DEPRECATED_PARSER = true;

        }
        else if (event instanceof Event) {
            BINLOG_PARSER_PROVIDER = BinlogEventParserProviderCode.SHYIKO;

            binlogEventV4 = null;

            binlogConnectorEvent = (Event) event;
            timestampOfBinlogEvent = binlogConnectorEvent.getHeader().getTimestamp();

            USING_DEPRECATED_PARSER = false;
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

    // timestamp received from OpenReplicator is in millisecond form,
    // but the millisecond part is actually 000 (for example 1447755881000)
    // TODO: verify that this is the same in Binlog Connector
    public long getTimestamp() {
        if (binlogEventV4 != null) {
            return binlogEventV4.getHeader().getTimestamp();
        }
        else {
            return binlogConnectorEvent.getHeader().getTimestamp();
        }
    }

    public String getBinlogFilename() {
        if (USING_DEPRECATED_PARSER) {
            return getOpenReplicatorEventBinlogFileName(binlogEventV4);
        }
        else {
            // TODO: need to do the whole dance as with open replicator, but more complicated
            // there is no binlog file name in the event, so need to buffer the last seen
            // binlog file name
            return ((RotateEventData) binlogConnectorEvent.getData()).getBinlogFilename();

        }
    }

    public void overrideTimestamp(long newTimestampValue) {
        this.timestampOfBinlogEvent = newTimestampValue;
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

    public String getQuerySQL() {
        if (USING_DEPRECATED_PARSER) {
            return    ((QueryEvent) binlogEventV4).getSql().toString();
        }
        else {
            return ((QueryEventData) binlogConnectorEvent.getData()).getSql();
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

    public RawEventType getEventType() {

        RawEventType t = RawEventType.UNKNOWN;

        if (binlogEventV4 != null) {
            switch (binlogEventV4.getHeader().getEventType()) {
                case MySQLConstants.QUERY_EVENT:
                    t = RawEventType.QUERY_EVENT;
                    break;
                case MySQLConstants.TABLE_MAP_EVENT:
                    t = RawEventType.TABLE_MAP_EVENT;
                    break;
                case MySQLConstants.UPDATE_ROWS_EVENT:
                case MySQLConstants.UPDATE_ROWS_EVENT_V2:
                    t = RawEventType.UPDATE_ROWS_EVENT;
                    break;
                case MySQLConstants.WRITE_ROWS_EVENT:
                case MySQLConstants.WRITE_ROWS_EVENT_V2:
                    t = RawEventType.WRITE_ROWS_EVENT;
                    break;
                case MySQLConstants.DELETE_ROWS_EVENT:
                case MySQLConstants.DELETE_ROWS_EVENT_V2:
                    t = RawEventType.DELETE_ROWS_EVENT;
                    break;
                case MySQLConstants.XID_EVENT:
                    t = RawEventType.XID_EVENT;
                    break;
                case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
                    t = RawEventType.FORMAT_DESCRIPTION_EVENT;
                    break;
                case MySQLConstants.ROTATE_EVENT:
                    t = RawEventType.ROTATE_EVENT;
                    break;
                case MySQLConstants.STOP_EVENT:
                    break;
                default:
                    t = RawEventType.UNKNOWN;
                    break;
            }
        }
        else {
            switch (binlogConnectorEvent.getHeader().getEventType()) {
                case QUERY:
                    t = RawEventType.QUERY_EVENT;
                    break;
                case TABLE_MAP:
                    t = RawEventType.TABLE_MAP_EVENT;
                    break;
                case PRE_GA_UPDATE_ROWS:
                case UPDATE_ROWS:
                case EXT_UPDATE_ROWS:
                    t = RawEventType.UPDATE_ROWS_EVENT;
                    break;
                case PRE_GA_WRITE_ROWS:
                case WRITE_ROWS:
                case EXT_WRITE_ROWS:
                    t = RawEventType.WRITE_ROWS_EVENT;
                    break;
                case PRE_GA_DELETE_ROWS:
                case DELETE_ROWS:
                case EXT_DELETE_ROWS:
                    t = RawEventType.DELETE_ROWS_EVENT;
                    break;
                case XID:
                    t = RawEventType.XID_EVENT;
                    break;
                case FORMAT_DESCRIPTION:
                    t = RawEventType.FORMAT_DESCRIPTION_EVENT;
                    break;
                case ROTATE:
                    t = RawEventType.ROTATE_EVENT;
                    break;
                case STOP:
                    t = RawEventType.STOP_EVENT;
                    break;
                default:
                    t = RawEventType.UNKNOWN;
                    break;
            }
        }
        return  t;
    }

    private String getOpenReplicatorEventBinlogFileName(BinlogEventV4 event) {

        switch (event.getHeader().getEventType()) {

            // Query Event:
            case MySQLConstants.QUERY_EVENT:
                return  ((QueryEvent) event).getBinlogFilename();

            // TableMap event:
            case MySQLConstants.TABLE_MAP_EVENT:
                return ((TableMapEvent) event).getBinlogFilename();

            case MySQLConstants.UPDATE_ROWS_EVENT:
            case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            case MySQLConstants.WRITE_ROWS_EVENT:
            case MySQLConstants.WRITE_ROWS_EVENT_V2:
            case MySQLConstants.DELETE_ROWS_EVENT:
            case MySQLConstants.DELETE_ROWS_EVENT_V2:
                return ((AbstractRowEvent) event).getBinlogFilename();

            case MySQLConstants.XID_EVENT:
                return ((XidEvent) event).getBinlogFilename();

            case MySQLConstants.ROTATE_EVENT:
                return ((RotateEvent) event).getBinlogFilename();

            case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
                return ((FormatDescriptionEvent) event).getBinlogFilename();

            case MySQLConstants.STOP_EVENT:
                return ((StopEvent) event).getBinlogFilename();

            default:
                // since it's not rotate event or format description event, the binlog file
                // has not changed, so return the last recorded
                return this.getCurrentPosition().getBinlogFilename();
        }
    }

    private long getOpenReplicatorEventBinlogPosition(BinlogEventV4 event) {

        switch (event.getHeader().getEventType()) {

            // Query Event:
            case MySQLConstants.QUERY_EVENT:
                return  ((QueryEvent) event).getHeader().getPosition();

            // TableMap event:
            case MySQLConstants.TABLE_MAP_EVENT:
                return ((TableMapEvent) event).getHeader().getPosition();

            case MySQLConstants.UPDATE_ROWS_EVENT:
            case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            case MySQLConstants.WRITE_ROWS_EVENT:
            case MySQLConstants.WRITE_ROWS_EVENT_V2:
            case MySQLConstants.DELETE_ROWS_EVENT:
            case MySQLConstants.DELETE_ROWS_EVENT_V2:
                return ((AbstractRowEvent) event).getHeader().getPosition();

            case MySQLConstants.XID_EVENT:
                return ((XidEvent) event).getHeader().getPosition();

            case MySQLConstants.ROTATE_EVENT:
                return ((RotateEvent) event).getHeader().getPosition();

            case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
                // workaround for a bug in open replicator which sets next position to 0, so
                // position turns out to be negative. Since it is always 4 for this event type,
                // we just use 4.
                return 4L;

            case MySQLConstants.STOP_EVENT:
                return ((StopEvent) event).getHeader().getPosition();

            default:
                return event.getHeader().getPosition();
        }
    }

    public BinlogEventV4 getBinlogEventV4() {
        return binlogEventV4;
    }

    public Event getBinlogConnectorEvent() {
        return binlogConnectorEvent;
    }
}
