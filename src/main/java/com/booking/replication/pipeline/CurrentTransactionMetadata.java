package com.booking.replication.pipeline;

import com.booking.replication.schema.exception.TableMapException;
import com.booking.replication.sql.QueryInspector;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.BinlogEventV4HeaderImpl;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.common.base.Joiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;
import java.util.*;

/**
 * Created by bosko on 11/10/15.
 */
public class CurrentTransactionMetadata {

    private static final Logger LOGGER = LoggerFactory.getLogger(CurrentTransactionMetadata.class);

    public static final long FAKEXID = 0;

    private UUID uuid = UUID.randomUUID();
    private long xid;
    private Map<Long,String> tableID2Name = new HashMap<>();
    private Map<Long, String> tableID2DBName = new HashMap<>();
    private QueryEvent beginEvent = null;
    private BinlogEventV4 finishEvent = null;

    private TableMapEvent firstMapEventInTransaction = null;
    private Queue<BinlogEventV4> events = new LinkedList<>();
    private long eventsCounter = 0;

    private final Map<String, TableMapEvent> currentTransactionTableMapEvents = new HashMap<>();

    public CurrentTransactionMetadata() {
    }

    public CurrentTransactionMetadata(QueryEvent event) {
        beginEvent = event;
    }

    @Override
    public String toString() {
        String beginEventBinlogFilename = null;
        Long beginEventBinlogPosition = null;
        if (beginEvent != null) {
            beginEventBinlogFilename = beginEvent.getBinlogFilename();
            beginEventBinlogPosition = beginEvent.getHeader().getPosition();
        }
        return "uuid: " + uuid +
                ", xid: " + xid +
                ", tableID2Name: " + tableID2Name +
                ", tableID2DBName: " + tableID2DBName +
                ", beginBinlogFileName: " + beginEventBinlogFilename +
                ", beginBinlogPosition: " + beginEventBinlogPosition +
                ", events size: " + events.size() +
                ", beginEvent: " + beginEvent +
                ", finishEvent: " + finishEvent +
                ", events:\n    - " + Joiner.on("\n    - ").join(events);
    }

    public UUID getUuid() {
        return uuid;
    }

    public long getXid() {
        return xid;
    }

    public void setXid(long xid) {
        this.xid = xid;
    }

    public QueryEvent getBeginEvent() {
        return beginEvent;
    }

    public void setFinishEvent(BinlogEventV4 finishEvent) {
        if (finishEvent instanceof XidEvent) {
            // xa-capable engines block (InnoDB)
            setXid(((XidEvent) finishEvent).getXid());
            doTimestampOverride(finishEvent.getHeader().getTimestamp());
            this.finishEvent = finishEvent;
        } else if (finishEvent instanceof QueryEvent && QueryInspector.getQueryEventType((QueryEvent) finishEvent) == "BEGIN") {
            // MyIsam block
            setXid(FAKEXID);
            doTimestampOverride(finishEvent.getHeader().getTimestamp());
            this.finishEvent = finishEvent;
        } else {
            throw new RuntimeException("Can't set finishEvent for transaction to a wrong event type: " + finishEvent);
        }
    }

    public BinlogEventV4 getFinishEvent() {
        return finishEvent;
    }

    public boolean hasBeginEvent() {
        return (beginEvent != null);
    }
    public boolean hasFinishEvent() {
        return (finishEvent != null);
    }

    public void addEvent(BinlogEventV4 event) {
        events.add(event);
        eventsCounter++;
    }

    public boolean hasEvents() {
        return (events.peek() != null);
    }

    public Queue<BinlogEventV4> getEvents() {
        return events;
    }

    public void doTimestampOverride(long timestamp) {
        for (BinlogEventV4 event : events) {
            ((BinlogEventV4HeaderImpl) event.getHeader()).setTimestamp(timestamp);
        }
    }

    public long getEventsCounter() {
        return eventsCounter;
    }

    /**
     * Update table map cache.
     */
    public void updateCache(TableMapEvent event) {
        LOGGER.debug("Updating cache. firstMapEventInTransaction: " + firstMapEventInTransaction + ", event: " + event);
        if (firstMapEventInTransaction == null) {
            firstMapEventInTransaction = event;
        }

        String tableName = event.getTableName().toString();

        tableID2Name.put(
                event.getTableId(),
                tableName
        );

        tableID2DBName.put(
                event.getTableId(),
                event.getDatabaseName().toString()
        );

        currentTransactionTableMapEvents.put(tableName, event);
    }

    /**
     * Map table id to table name.
     */
    public String getTableNameFromID(Long tableID) throws TableMapException {
        if (! tableID2DBName.containsKey(tableID)) {
            LOGGER.error(String.format(
                    "Table ID not known. Known tables and ids are: %s",
                    Joiner.on(" ").join(tableID2DBName.keySet(), " ")));
            throw new TableMapException("Table ID not present in CurrentTransactionMetadata!");
        }

        return tableID2Name.get(tableID);
    }

    /**
     * Map table id to schema name.
     */
    public String getDBNameFromTableID(Long tableID) throws TableMapException {
        String dbName = tableID2DBName.get(tableID);

        if (dbName == null) {
            throw new TableMapException("Table ID not present in CurrentTransactionMetadata!");
        } else {
            return dbName;
        }
    }

    public TableMapEvent getTableMapEvent(String tableName) {
        return currentTransactionTableMapEvents.get(tableName);
    }

    public TableMapEvent getFirstMapEventInTransaction() {
        return firstMapEventInTransaction;
    }

    public boolean hasMappingInTransaction() {
        return firstMapEventInTransaction != null;
    }


    public Map<String, TableMapEvent> getCurrentTransactionTableMapEvents() {
        return currentTransactionTableMapEvents;
    }

}
