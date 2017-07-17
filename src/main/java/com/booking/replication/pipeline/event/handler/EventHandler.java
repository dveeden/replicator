package com.booking.replication.pipeline.event.handler;


import com.booking.replication.pipeline.PipelineOrchestrator;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.common.util.MySQLConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by edmitriev on 7/13/17.
 */
public class EventHandler implements AbstractHandler<BinlogEventV4> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventHandler.class);

    private final PipelineOrchestrator pipelineOrchestrator;
    private final QueryEventHandler queryEventHandler;
    private final TableMapEventHandler tableMapEventHandler;
    private final UpdateRowsEventHandler updateRowsEventHandler;
    private final WriteRowsEventHandler writeRowsEventHandler;
    private final DeleteRowsEventHandler deleteRowsEventHandler;
    private final XidEventHandler xidEventHandler;
    private final FormatDescriptionEventHandler formatDescriptionEventHandler;
    private final RotateEventHandler rotateEventHandler;


    public EventHandler(PipelineOrchestrator pipelineOrchestrator, QueryEventHandler queryEventHandler,
                        TableMapEventHandler tableMapEventHandler, UpdateRowsEventHandler updateRowsEventHandler,
                        WriteRowsEventHandler writeRowsEventHandler, DeleteRowsEventHandler deleteRowsEventHandler,
                        XidEventHandler xidEventHandler, FormatDescriptionEventHandler formatDescriptionEventHandler,
                        RotateEventHandler rotateEventHandler) {
        this.pipelineOrchestrator = pipelineOrchestrator;
        this.queryEventHandler = queryEventHandler;
        this.tableMapEventHandler = tableMapEventHandler;
        this.updateRowsEventHandler = updateRowsEventHandler;
        this.writeRowsEventHandler = writeRowsEventHandler;
        this.deleteRowsEventHandler = deleteRowsEventHandler;
        this.xidEventHandler = xidEventHandler;
        this.formatDescriptionEventHandler = formatDescriptionEventHandler;
        this.rotateEventHandler = rotateEventHandler;
    }

    @Override
    public void apply(BinlogEventV4 event, long xid) {
        LOGGER.debug("Applying event: " + event);
        try {
            switch (event.getHeader().getEventType()) {
                // Check for DDL and pGTID:
                case MySQLConstants.QUERY_EVENT:
                    queryEventHandler.apply((QueryEvent) event, xid);
                    break;
                case MySQLConstants.TABLE_MAP_EVENT:
                    tableMapEventHandler.apply((TableMapEvent) event, xid);
                    break;
                case MySQLConstants.UPDATE_ROWS_EVENT:
                case MySQLConstants.UPDATE_ROWS_EVENT_V2:
                    // Data event
                    updateRowsEventHandler.apply((AbstractRowEvent) event, xid);
                    break;
                case MySQLConstants.WRITE_ROWS_EVENT:
                case MySQLConstants.WRITE_ROWS_EVENT_V2:
                    // Data event
                    writeRowsEventHandler.apply((AbstractRowEvent) event, xid);
                    break;
                case MySQLConstants.DELETE_ROWS_EVENT:
                case MySQLConstants.DELETE_ROWS_EVENT_V2:
                    // Data event
                    deleteRowsEventHandler.apply((AbstractRowEvent) event, xid);
                    break;
                case MySQLConstants.XID_EVENT:
                    // Later we may want to tag previous data events with xid_id
                    // (so we can know if events were in the same transaction).
                    xidEventHandler.apply((XidEvent) event, xid);
                    break;
                case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
                    formatDescriptionEventHandler.apply((FormatDescriptionEvent) event, xid);
                    break;
                case MySQLConstants.ROTATE_EVENT:
                    // flush buffer at the end of binlog file
                    rotateEventHandler.apply((RotateEvent) event, xid);
                    break;
                case MySQLConstants.STOP_EVENT:
                    // Events that we expect to appear in the binlog, but we don't do
                    // any extra processing.
                    break;
                default:
                    // Events that we do not expect to appear in the binlog
                    // so a warning should be logged for those types
                    LOGGER.warn("Unexpected event type: " + event.getHeader().getEventType());
                    break;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to apply event: " + event.toString(), e);
            pipelineOrchestrator.requestShutdown();
        }
    }

    @Override
    public void handle(BinlogEventV4 event) {
        LOGGER.debug("Handling event: " + event);
        try {
            switch (event.getHeader().getEventType()) {
                // Check for DDL and pGTID:
                case MySQLConstants.QUERY_EVENT:
                    queryEventHandler.handle((QueryEvent) event);
                    break;
                case MySQLConstants.TABLE_MAP_EVENT:
                    tableMapEventHandler.handle((TableMapEvent) event);
                    break;
                case MySQLConstants.UPDATE_ROWS_EVENT:
                case MySQLConstants.UPDATE_ROWS_EVENT_V2:
                    // Data event
                    updateRowsEventHandler.handle((AbstractRowEvent) event);
                    break;
                case MySQLConstants.WRITE_ROWS_EVENT:
                case MySQLConstants.WRITE_ROWS_EVENT_V2:
                    // Data event
                    writeRowsEventHandler.handle((AbstractRowEvent) event);
                    break;
                case MySQLConstants.DELETE_ROWS_EVENT:
                case MySQLConstants.DELETE_ROWS_EVENT_V2:
                    // Data event
                    deleteRowsEventHandler.handle((AbstractRowEvent) event);
                    break;
                case MySQLConstants.XID_EVENT:
                    // Later we may want to tag previous data events with xid_id
                    // (so we can know if events were in the same transaction).
                    xidEventHandler.handle((XidEvent) event);
                    break;
                case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
                    formatDescriptionEventHandler.handle((FormatDescriptionEvent) event);
                    break;
                case MySQLConstants.ROTATE_EVENT:
                    // flush buffer at the end of binlog file
                    rotateEventHandler.handle((RotateEvent) event);
                    break;
                case MySQLConstants.STOP_EVENT:
                    // Events that we expect to appear in the binlog, but we don't do
                    // any extra processing.
                    break;
                default:
                    // Events that we do not expect to appear in the binlog
                    // so a warning should be logged for those types
                    LOGGER.warn("Unexpected event type: " + event.getHeader().getEventType());
                    break;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to handle event: " + event.toString(), e);
            pipelineOrchestrator.requestShutdown();
        }
    }
}
