package com.booking.replication.binlog.event;

import com.booking.replication.binlog.common.ExtractedColumn;
import com.booking.replication.binlog.common.ExtractedColumnBytes;
import com.booking.replication.binlog.common.ExtractedRow;
import com.booking.replication.binlog.common.column.AnyColumn;
import com.booking.replication.schema.exception.TableMapException;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.column.*;
import com.google.code.or.common.util.MySQLConstants;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Created by bosko on 6/1/17.
 */
public class RawBinlogEvent_Write extends RawBinlogEvent_Rows {

    List<ExtractedRow> extractedRows;

    public RawBinlogEvent_Write(Object event) throws Exception {
        super(event);
        extractedRows = this.extractRowsFromEvent();
    }

    public int getColumnCount() {
        if (this.USING_DEPRECATED_PARSER) {
            return ((WriteRowsEvent) binlogEventV4).getColumnCount().intValue();
        }
        else {
            BitSet includedColumns = ((WriteRowsEventData) binlogConnectorEvent.getData()).getIncludedColumns();
            return includedColumns.cardinality();
        }
    }

    public long getTableId() {
        if (this.USING_DEPRECATED_PARSER) {
            return ((WriteRowsEvent) binlogEventV4).getTableId();
        }
        else {
            return ((WriteRowsEventData) binlogConnectorEvent.getData()).getTableId();
        }
    }

    public List<ExtractedRow> getExtractedRows() {
        return extractedRows;
    }

    private List<ExtractedRow> extractRowsFromEvent() throws Exception {

        if (this.USING_DEPRECATED_PARSER) {

            List<ExtractedRow> extractedRows = new ArrayList();

            for (com.google.code.or.common.glossary.Row orRow : ((WriteRowsEvent) binlogEventV4).getRows()) {

                List<ExtractedColumn> extractedColumns = new ArrayList<>();

                for (Column column: orRow.getColumns()) {

                    byte[] columnBytes;
                    int columnType;

                    if (column instanceof BitColumn) {
                        columnType = MySQLConstants.TYPE_BIT;
                        columnBytes = ((BitColumn)column).getValue();
                    }
                    else if (column instanceof BlobColumn) {
                        columnType = MySQLConstants.TYPE_BLOB;
                        columnBytes = ((BlobColumn)column).getValue();
                    }
                    else if (column instanceof StringColumn) {
                        columnType = MySQLConstants.TYPE_STRING;
                        columnBytes = ((StringColumn)column).getValue();
                    }
                    else if (column instanceof NullColumn) {
                        columnType = MySQLConstants.TYPE_NULL;
                        columnBytes = null;
                    }
                    else if (column instanceof SetColumn) {
                        columnType = MySQLConstants.TYPE_SET;
                        long value = ((SetColumn)column).getValue();
                        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                        buffer.putLong(value);
                        columnBytes = buffer.array();
                    }
                    else  if (column instanceof  EnumColumn) {
                        columnType = MySQLConstants.TYPE_ENUM;
                    }
                    else if (column instanceof DecimalColumn) {
                        columnType = MySQLConstants.TYPE_DECIMAL;
                    }
                    else if (column instanceof DoubleColumn) {
                        columnType = MySQLConstants.TYPE_DOUBLE;
                    }
                    else if (column instanceof FloatColumn) {
                        columnType = MySQLConstants.TYPE_FLOAT;
                    }
                    else if (column instanceof TinyColumn) {
                        columnType = MySQLConstants.TYPE_TINY;
                    }
                    else if (column instanceof  ShortColumn) {
                        columnType = MySQLConstants.TYPE_SHORT;
                    }
                    else if (column instanceof Int24Column) {
                        columnType = MySQLConstants.TYPE_INT24;
                    }
                    else if (column instanceof LongColumn) {
                        columnType = MySQLConstants.TYPE_LONG;
                    }
                    else if (column instanceof LongLongColumn) {
                        columnType = MySQLConstants.TYPE_LONGLONG;
                    }
                    else if (column instanceof YearColumn) {
                        columnType = MySQLConstants.TYPE_YEAR;
                    }
                    else if (column instanceof DateColumn) {
                        columnType = MySQLConstants.TYPE_DATE;
                    }
                    else if (column instanceof DatetimeColumn) {
                        columnType = MySQLConstants.TYPE_DATETIME;
                    }
                    else if (column instanceof Datetime2Column) {
                        columnType = MySQLConstants.TYPE_DATETIME2;
                    }
                    else if (column instanceof TimeColumn) {
                        columnType = MySQLConstants.TYPE_TIME;
                    }
                    else if (column instanceof  Time2Column) {
                        columnType = MySQLConstants.TYPE_TIME2;
                    }
                    else if (column instanceof TimestampColumn) {
                        columnType = MySQLConstants.TYPE_TIMESTAMP;
                    }
                    else if (column instanceof Timestamp2Column) {
                        columnType = MySQLConstants.TYPE_TIMESTAMP2;
                    } else {
                        throw new Exception("Unknown MySQL type in the Open Replicator event" + column.getClass() + " Object = " + column);
                    }
                    ExtractedColumn extractedColumn = new ExtractedColumnBytes(columnType, columnBytes);

                    extractedColumns.add(extractedColumn);

                }

                ExtractedRow extractedRow = new ExtractedRow(extractedColumns);
                extractedRows.add(extractedRow);
            }
            return extractedRows;
        }
        else {
            List<ExtractedRow> rows = new ArrayList();
            for (Serializable[] bcRow: ((WriteRowsEventData) binlogConnectorEvent.getData()).getRows()) {
                com.github.shyiko.mysql.binlog.event.deserialization.AbstractRowsEventDataDeserializer

                        bcRow
            }
            return rows;
        }
    }
}
