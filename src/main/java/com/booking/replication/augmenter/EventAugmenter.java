package com.booking.replication.augmenter;

import static com.codahale.metrics.MetricRegistry.name;

import com.booking.replication.Metrics;
import com.booking.replication.binlog.common.Cell;
import com.booking.replication.binlog.common.Row;
import com.booking.replication.binlog.event.*;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.schema.ActiveSchemaVersion;
import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.column.types.Converter;
import com.booking.replication.schema.exception.SchemaTransitionException;
import com.booking.replication.schema.exception.TableMapException;
import com.booking.replication.schema.table.TableSchemaVersion;

import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;

/**
 * EventAugmenter
 *
 * <p>This class contains the logic that tracks the schema
 * that corresponds to current binlog position. It also
 * handles schema transition management when DDL statement
 * is encountered. In addition it maintains a tableMapEvent
 * cache (that is needed to getValue tableName from tableID) and
 * provides utility method for mapping raw binlog event to
 * currently active schema.</p>
 */
public class EventAugmenter {

    // public CurrentTransactionMetadata currentTransactionMetadata;

    private ActiveSchemaVersion activeSchemaVersion;

    private static final Logger LOGGER = LoggerFactory.getLogger(EventAugmenter.class);

    /**
     * Event Augmenter constructor.
     *
     * @param asv Active schema version
     */
    public EventAugmenter(ActiveSchemaVersion asv) throws SQLException, URISyntaxException {
        activeSchemaVersion = asv;
    }

    /**
     * Get active schema version.
     *
     * @return ActiveSchemaVersion
     */
    public ActiveSchemaVersion getActiveSchemaVersion() {
        return activeSchemaVersion;
    }

    public HashMap<String, String> getSchemaTransitionSequence(RawBinlogEvent event) throws SchemaTransitionException {

        if (event.isQuery()) {

            String ddl = ((RawBinlogEvent_Query) event).getSql();

            // query
            HashMap<String, String> sqlCommands = new HashMap<>();
            sqlCommands.put("databaseName", ((RawBinlogEvent_Query) event).getDatabaseName());
            sqlCommands.put("originalDDL", ddl);

            sqlCommands.put(
                    "ddl",
                    rewriteActiveSchemaName( // since active schema has a postfix, we need to make sure that queires that
                            ddl,             // specify schema explicitly are rewritten so they work properly on active schema
                            ((RawBinlogEvent_Query) event).getDatabaseName().toString()
                    ));

                // handle timezone overrides during schema changes
                if (((RawBinlogEvent_Query) event).hasTimezoneOverride()) {

                    HashMap<String,String> timezoneOverrideCommands = ((RawBinlogEvent_Query) event).getTimezoneOverrideCommands();

                    if (timezoneOverrideCommands.containsKey("timezonePre")) {
                        sqlCommands.put("timezonePre", timezoneOverrideCommands.get("timezonePre"));
                    }
                    if (timezoneOverrideCommands.containsKey("timezonePost")) {
                        sqlCommands.put("timezonePost",  timezoneOverrideCommands.get("timezonePost"));
                    }
                }

            return sqlCommands;

        } else {
            throw new SchemaTransitionException("Not a valid query event!");
        }
    }

    /**
     * Mangle name of the active schema before applying DDL statements.
     *
     * @param query             Query string
     * @param replicantDbName   Database name
     * @return                  Rewritten query
     */
    public String rewriteActiveSchemaName(String query, String replicantDbName) {
        String dbNamePattern = "( " + replicantDbName + ".)|(`" + replicantDbName + "`.)";
        query = query.replaceAll(dbNamePattern, " ");

        return query;
    }

    /**
     * Map data event to Schema.
     *
     * <p>Maps raw binlog event to column names and types</p>
     *
     * @param  event               AbstractRowEvent
     * @return augmentedDataEvent  AugmentedRow
     */
    public AugmentedRowsEvent mapDataEventToSchema(RawBinlogEvent_Rows event, PipelineOrchestrator caller) throws TableMapException {

        AugmentedRowsEvent au;

        switch (event.getEventType()) {
            case UPDATE_ROWS_EVENT:
                RawBinlogEvent_Update updateRowsEvent = ((RawBinlogEvent_Update) event);
                au = augmentUpdateRowsEventV2(updateRowsEvent, caller);
                break;
            case WRITE_ROWS_EVENT:
                RawBinlogEvent_Write writeRowsEvent = ((RawBinlogEvent_Write) event);
                au = augmentWriteRowsEventV2(writeRowsEvent, caller);
                break;
            case DELETE_ROWS_EVENT:
                RawBinlogEvent_Delete deleteRowsEvent = ((RawBinlogEvent_Delete) event);
                au = augmentDeleteRowsEventV2(deleteRowsEvent, caller);
                break;
            default:
                throw new TableMapException("RBR event type expected! Received type: " + event.getEventType().toString(), event);
        }

        if (au == null) {
            throw  new TableMapException("Augmented event ended up as null - something went wrong!", event);
        }

        return au;
    }

    private AugmentedRowsEvent augmentWriteRowsEvent(RawBinlogEvent_Write writeRowsEvent, PipelineOrchestrator caller) throws TableMapException {

        // table name
        String tableName = caller.currentTransactionMetadata.getTableNameFromID(writeRowsEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchemaVersion tableSchemaVersion = activeSchemaVersion.getActiveSchemaTables().get(tableName);

        if (tableSchemaVersion == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", writeRowsEvent);
        }

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(writeRowsEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = writeRowsEvent.getColumnCount();

        // In write event there is only a List<ParsedRow> from getRows. No before after naturally.

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event
        for (Row row : writeRowsEvent.getExtractedRows()) {

            String evType = "INSERT";
            rowBinlogEventOrdinal++;

            AugmentedRow augEvent = new AugmentedRow(
                    augEventGroup.getBinlogFileName(),
                    rowBinlogEventOrdinal,
                    tableName,
                    tableSchemaVersion,
                    evType,
                    writeRowsEvent.getHeader()
            );

            tableMetrics.inserted.inc();
            tableMetrics.processed.inc();

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                String columnName = tableSchemaVersion.getColumnIndexToNameMap().get(columnIndex);

                // but here index goes from 0..
                Column columnValue = row.getRowCells().get(columnIndex - 1);

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchemaVersion.getColumnSchemaByColumnName(columnName);

                String value = Converter.cellValueToString(columnValue, columnSchema);

                augEvent.addColumnDataForInsert(columnName, value, columnSchema.getColumnType());
            }
            augEventGroup.addSingleRowEvent(augEvent);
        }

        return augEventGroup;
    }

    // ==============================================================================================
    //
    // TODO: refactor these functions since they are mostly the same. Also move to a different class.
    // Same as for V1 write event. There is some extra data in V2, but not sure if we can use it.
    private AugmentedRowsEvent augmentWriteRowsEventV2(
            RawBinlogEvent_Write writeRowsEvent,
            PipelineOrchestrator caller) throws TableMapException {

        // table name
        String tableName = caller.currentTransactionMetadata.getTableNameFromID(writeRowsEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchemaVersion tableSchemaVersion = activeSchemaVersion.getActiveSchemaTables().get(tableName);

        if (tableSchemaVersion == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", writeRowsEvent);
        }

        int numberOfColumns = writeRowsEvent.getColumnCount();

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(writeRowsEvent);
        augEventGroup.setMysqlTableName(tableName);

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event
        for (Row row : writeRowsEvent.getExtractedRows()) {

            String evType = "INSERT";
            rowBinlogEventOrdinal++;

            AugmentedRow augEvent = new AugmentedRow(
                augEventGroup.getBinlogFileName(),
                rowBinlogEventOrdinal,
                tableName,
                tableSchemaVersion,
                evType,
                writeRowsEvent.getHeader()
            );

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                // getValue column name from indexToNameMap
                String columnName = tableSchemaVersion.getColumnIndexToNameMap().get(columnIndex);

                // but here index goes from 0..
                Column columnValue = row.getRowCells().get(columnIndex - 1);

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchemaVersion.getColumnSchemaByColumnName(columnName);

                // type cast
                String value = Converter.cellValueToString(columnValue, columnSchema);

                augEvent.addColumnDataForInsert(columnName, value, columnSchema.getColumnType());
            }
            augEventGroup.addSingleRowEvent(augEvent);

            tableMetrics.inserted.inc();
            tableMetrics.processed.inc();
        }

        return augEventGroup;
    }

    private AugmentedRowsEvent augmentDeleteRowsEvent(RawBinlogEvent_Delete deleteRowsEvent, PipelineOrchestrator pipeline)
            throws TableMapException {

        // table name
        String tableName = pipeline.currentTransactionMetadata.getTableNameFromID(deleteRowsEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchemaVersion tableSchemaVersion = activeSchemaVersion.getActiveSchemaTables().get(tableName);

        // TODO: refactor
        if (tableSchemaVersion == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", deleteRowsEvent);
        }
        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(deleteRowsEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = deleteRowsEvent.getColumnCount();

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event
        for (Row row : deleteRowsEvent.getExtractedRows()) {

            String evType =  "DELETE";
            rowBinlogEventOrdinal++;
            AugmentedRow augEvent = new AugmentedRow(
                    augEventGroup.getBinlogFileName(),
                    rowBinlogEventOrdinal,
                    tableName,
                    tableSchemaVersion,
                    evType,
                    deleteRowsEvent.getPosition()
            );

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                String columnName = tableSchemaVersion.getColumnIndexToNameMap().get(columnIndex);

                // but here index goes from 0..
                Cell cellValue = row.getRowCells().get(columnIndex - 1);

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchemaVersion.getColumnSchemaByColumnName(columnName);

                String value = Converter.cellValueToString(cellValue, columnSchema);

                augEvent.addColumnDataForInsert(columnName, value, columnSchema.getColumnType());
            }
            augEventGroup.addSingleRowEvent(augEvent);

            tableMetrics.processed.inc();
            tableMetrics.deleted.inc();
        }

        return augEventGroup;
    }

    // For now this is the same as for V1 event.
    private AugmentedRowsEvent augmentDeleteRowsEventV2(
            RawBinlogEvent_Delete deleteRowsEvent,
            PipelineOrchestrator caller) throws TableMapException {
        // table name
        String tableName = caller.currentTransactionMetadata.getTableNameFromID(deleteRowsEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchemaVersion tableSchemaVersion = activeSchemaVersion.getActiveSchemaTables().get(tableName);

        // TODO: refactor
        if (tableSchemaVersion == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", deleteRowsEvent);
        }

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(deleteRowsEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = deleteRowsEvent.getColumnCount().intValue();

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event
        for (com.google.code.or.common.glossary.Row row : deleteRowsEvent.getRows()) {

            String evType = "DELETE";
            rowBinlogEventOrdinal++;

            AugmentedRow augEvent = new AugmentedRow(
                    augEventGroup.getBinlogFileName(),
                    rowBinlogEventOrdinal,
                    tableName,
                    tableSchemaVersion,
                    evType,
                    deleteRowsEvent.getHeader()
            );

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                String columnName = tableSchemaVersion.getColumnIndexToNameMap().get(columnIndex);

                // but here index goes from 0..
                Column columnValue = row.getColumns().get(columnIndex - 1);

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchemaVersion.getColumnSchemaByColumnName(columnName);

                String value = Converter.cellValueToString(columnValue, columnSchema);

                // TODO: delete has same content as insert, but add a differently named method for clarity
                augEvent.addColumnDataForInsert(columnName, value, columnSchema.getColumnType());
            }
            augEventGroup.addSingleRowEvent(augEvent);

            tableMetrics.deleted.inc();
            tableMetrics.processed.inc();
        }

        return augEventGroup;
    }

    private AugmentedRowsEvent augmentUpdateRowsEvent(UpdateRowsEvent upEvent, PipelineOrchestrator caller) throws TableMapException {

        // table name
        String tableName = caller.currentTransactionMetadata.getTableNameFromID(upEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchemaVersion tableSchemaVersion = activeSchemaVersion.getActiveSchemaTables().get(tableName);

        // TODO: refactor
        if (tableSchemaVersion == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", upEvent);
        }

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(upEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = upEvent.getColumnCount().intValue();

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event

        // rowPair is pair <rowBeforeChange, rowAfterChange>
        for (Pair<com.google.code.or.common.glossary.Row> rowPair : upEvent.getRows()) {

            String evType = "UPDATE";
            rowBinlogEventOrdinal++;

            AugmentedRow augEvent = new AugmentedRow(
                augEventGroup.getBinlogFileName(),
                rowBinlogEventOrdinal,
                tableName,
                    tableSchemaVersion,
                evType,
                upEvent.getHeader()
            );

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                String columnName = tableSchemaVersion.getColumnIndexToNameMap().get(columnIndex);

                // but here index goes from 0..
                Column columnValueBefore = rowPair.getBefore().getColumns().get(columnIndex - 1);
                Column columnValueAfter = rowPair.getAfter().getColumns().get(columnIndex - 1);

                // We need schema for proper type casting; Since this is RowChange event, schema
                // is the same for both before and after states
                ColumnSchema columnSchema = tableSchemaVersion.getColumnSchemaByColumnName(columnName);

                String valueBefore = Converter.cellValueToString(columnValueBefore, columnSchema);
                String valueAfter  = Converter.cellValueToString(columnValueAfter, columnSchema);

                String columnType  = columnSchema.getColumnType();

                augEvent.addColumnDataForUpdate(columnName, valueBefore, valueAfter, columnType);
            }
            augEventGroup.addSingleRowEvent(augEvent);

            tableMetrics.processed.inc();
            tableMetrics.updated.inc();
        }

        return augEventGroup;
    }

    // For now this is the same as V1. Not sure if the extra info in V2 can be of use to us.
    private AugmentedRowsEvent augmentUpdateRowsEventV2(RawBinlogEvent_Update upEvent, PipelineOrchestrator caller) throws TableMapException {

        // table name
        String tableName = caller.currentTransactionMetadata.getTableNameFromID(upEvent.getTableId());

        PerTableMetrics tableMetrics = PerTableMetrics.get(tableName);

        // getValue schema for that table from activeSchemaVersion
        TableSchemaVersion tableSchemaVersion = activeSchemaVersion.getActiveSchemaTables().get(tableName);

        // TODO: refactor
        if (tableSchemaVersion == null) {
            throw new TableMapException("Table schema not initialized for table " + tableName + ". Cant proceed.", upEvent);
        }

        AugmentedRowsEvent augEventGroup = new AugmentedRowsEvent(upEvent);
        augEventGroup.setMysqlTableName(tableName);

        int numberOfColumns = upEvent.getColumnCount().intValue();

        long rowBinlogEventOrdinal = 0; // order of the row in the binlog event

        // rowPair is pair <rowBeforeChange, rowAfterChange>
        for (Pair<com.google.code.or.common.glossary.Row> rowPair : upEvent.getRows()) {

            String evType = "UPDATE";
            rowBinlogEventOrdinal++;

            AugmentedRow augEvent = new AugmentedRow(
                augEventGroup.getBinlogFileName(),
                rowBinlogEventOrdinal,
                tableName,
                    tableSchemaVersion,
                evType,
                upEvent.getHeader()
            );

            //column index counting starts with 1
            for (int columnIndex = 1; columnIndex <= numberOfColumns ; columnIndex++ ) {

                String columnName = tableSchemaVersion.getColumnIndexToNameMap().get(columnIndex);

                if (columnName == null) {
                    LOGGER.error("null columnName for { columnIndex => " + columnIndex + ", tableName => " + tableName + " }" );
                    throw new TableMapException("columnName cant be null", upEvent);
                }

                // but here index goes from 0..
                Column columnValueBefore = rowPair.getBefore().getColumns().get(columnIndex - 1);
                Column columnValueAfter = rowPair.getAfter().getColumns().get(columnIndex - 1);

                // We need schema for proper type casting
                ColumnSchema columnSchema = tableSchemaVersion.getColumnSchemaByColumnName(columnName);

                try {
                    String valueBefore = Converter.cellValueToString(columnValueBefore, columnSchema);
                    String valueAfter  = Converter.cellValueToString(columnValueAfter, columnSchema);
                    String columnType  = columnSchema.getColumnType();

                    augEvent.addColumnDataForUpdate(columnName, valueBefore, valueAfter, columnType);
                } catch (TableMapException e) {
                    TableMapException rethrow = new TableMapException(e.getMessage(), upEvent);
                    rethrow.setStackTrace(e.getStackTrace());
                    throw rethrow;
                }
            }
            augEventGroup.addSingleRowEvent(augEvent);

            tableMetrics.processed.inc();
            tableMetrics.updated.inc();
        }
        return augEventGroup;
    }

    private static class PerTableMetrics {
        private static String prefix = "mysql";
        private static HashMap<String, PerTableMetrics> tableMetricsHash = new HashMap<>();

        static PerTableMetrics get(String tableName) {
            if (! tableMetricsHash.containsKey(tableName)) {
                tableMetricsHash.put(tableName, new PerTableMetrics(tableName));
            }
            return tableMetricsHash.get(tableName);
        }

        final Counter inserted;
        final Counter processed;
        final Counter deleted;
        final Counter updated;
        final Counter committed;

        PerTableMetrics(String tableName) {
            inserted    = Metrics.registry.counter(name(prefix, tableName, "inserted"));
            processed   = Metrics.registry.counter(name(prefix, tableName, "processed"));
            deleted     = Metrics.registry.counter(name(prefix, tableName, "deleted"));
            updated     = Metrics.registry.counter(name(prefix, tableName, "updated"));
            committed   = Metrics.registry.counter(name(prefix, tableName, "committed"));
        }
    }

}
