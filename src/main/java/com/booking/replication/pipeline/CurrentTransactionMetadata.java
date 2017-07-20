package com.booking.replication.pipeline;

import com.booking.replication.schema.exception.TableMapException;
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

    private UUID uuid = UUID.randomUUID();
    private long xid;
    private Map<Long,String> tableID2Name = new HashMap<>();
    private Map<Long, String> tableID2DBName = new HashMap<>();

    private TableMapEvent firstMapEventInTransaction = null;
    private Queue<BinlogEventV4> events = new LinkedList<>();
    private long eventsCounter = 0;

    private final Map<String, TableMapEvent> currentTransactionTableMapEvents = new HashMap<>();

    @Override
    public String toString() {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("uuid: " + uuid);
        stringBuffer.append(", xid: " + xid);
        stringBuffer.append(", tableID2Name: " + tableID2Name);
        stringBuffer.append(", tableID2DBName: " + tableID2DBName);
        stringBuffer.append(", events size: " + events.size());
        stringBuffer.append(", events:\n    - " + Joiner.on("\n    - ").join(events));
        return stringBuffer.toString();
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


    public void addEvent(BinlogEventV4 event) {
        events.add(event);
        eventsCounter++;
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
