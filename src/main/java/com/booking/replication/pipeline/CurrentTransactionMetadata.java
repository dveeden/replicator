package com.booking.replication.pipeline;

import com.booking.replication.binlog.EventPosition;
import com.booking.replication.pipeline.event.handler.TransactionException;
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

import java.util.*;

/**
 * Created by bosko on 11/10/15.
 */
public class CurrentTransactionMetadata {

    private static final Logger LOGGER = LoggerFactory.getLogger(CurrentTransactionMetadata.class);

    public static final long FAKEXID = 0;

    private final UUID uuid = UUID.randomUUID();
    private long xid;
    private final Map<Long,String> tableID2Name = new HashMap<>();
    private final Map<Long, String> tableID2DBName = new HashMap<>();
    private QueryEvent beginEvent = null;
    private BinlogEventV4 finishEvent = null;
    private boolean isRewinded = false;

    private TableMapEvent firstMapEventInTransaction = null;
    private Queue<BinlogEventV4> events = new LinkedList<>();

    private final Map<String, TableMapEvent> currentTransactionTableMapEvents = new HashMap<>();

    public CurrentTransactionMetadata() {
    }

    public CurrentTransactionMetadata(QueryEvent event) {
        if (!QueryInspector.getQueryEventType((QueryEvent) event).equals("BEGIN")) {
            throw new RuntimeException("Can't set beginEvent for transaction to a wrong event type: " + event);
        }
        beginEvent = event;
    }

    @Override
    public String toString() {
        String beginEventBinlogFilename = beginEvent == null ? null : EventPosition.getEventBinlogFileName(beginEvent);
        Long beginEventBinlogPosition = beginEvent == null ? null : EventPosition.getEventBinlogPosition(beginEvent);
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


    public void setFinishEvent(XidEvent finishEvent) throws TransactionException {
        // xa-capable engines block (InnoDB)
        if (isRewinded) {
            if (this.finishEvent != null && ((XidEvent) this.finishEvent).getXid() != finishEvent.getXid()) {
                throw new TransactionException("XidEvents must match if in rewinded transaction. Old: " + this.finishEvent + ", new: " + finishEvent);
            }
        }
        setXid(finishEvent.getXid());
        doTimestampOverride(finishEvent.getHeader().getTimestamp());
        this.finishEvent = finishEvent;
    }

    public void setFinishEvent(BinlogEventV4 finishEvent) throws TransactionException {
        // MyIsam block
        if (!QueryInspector.getQueryEventType((QueryEvent) finishEvent).equals("COMMIT")) {
            throw new TransactionException("Can't set finishEvent for transaction to a wrong event type: " + finishEvent);
        }
        setXid(FAKEXID);
        doTimestampOverride(finishEvent.getHeader().getTimestamp());
        this.finishEvent = finishEvent;
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
    }

    public boolean hasEvents() {
        return (events.peek() != null);
    }

    public Queue<BinlogEventV4> getEvents() {
        return events;
    }

    public void clearEvents() {
        events = new LinkedList<>();
    }

    public void doTimestampOverride(long timestamp) {
        for (BinlogEventV4 event : events) {
            ((BinlogEventV4HeaderImpl) event.getHeader()).setTimestamp(timestamp);
        }
    }

    public void setEventsTimestampToFinishEvent() throws TransactionException {
        if (!hasFinishEvent()) {
            throw new TransactionException("Can't set timestamp to timestamp of finish event while no finishEvent exists");
        }
        doTimestampOverride(finishEvent.getHeader().getTimestamp());
    }

    public long getEventsCounter() {
        return events.size();
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

    public boolean isRewinded() {
        return isRewinded;
    }

    public void setRewinded(boolean rewinded) {
        isRewinded = rewinded;
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
