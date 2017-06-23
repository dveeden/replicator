package com.booking.replication.binlog.event;

import com.booking.replication.binlog.common.Cell;
import com.booking.replication.binlog.common.ExtractedColumnBytes;
import com.booking.replication.binlog.common.Row;
import com.booking.replication.binlog.common.cell.AnyColumn;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.column.*;
import com.google.code.or.common.util.MySQLConstants;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Created by bosko on 6/1/17.
 */
public class RawBinlogEvent_Write extends RawBinlogEvent_Rows {

    List<Row> extractedRows;

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

    public List<Row> getExtractedRows() {
        return extractedRows;
    }

    private List<Row> extractRowsFromEvent() throws Exception {

        if (this.USING_DEPRECATED_PARSER) {

            List<Row> extractedRows = new ArrayList();

            for (com.google.code.or.common.glossary.Row orRow : ((WriteRowsEvent) binlogEventV4).getRows()) {

                List<Cell> rowCells = new ArrayList<>();

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
                        columnType = MySQLConstants.TYPE_SET;
                        Integer value = ((EnumColumn)column).getValue();
                        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                        buffer.putLong(value);
                        columnBytes = buffer.array();
                    }
                    else if (column instanceof DecimalColumn) {
                        columnType = MySQLConstants.TYPE_DECIMAL;
                        BigDecimal value =  ((DecimalColumn)column).getValue();
                        int precision =  ((DecimalColumn)column).getPrecision();
                        int scale =  ((DecimalColumn)column).getScale();


                        // TODO: return BigDecimal wraped in type class
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
                    Cell rowCell = new ExtractedColumnBytes(columnType, columnBytes);

                    rowCells.add(rowCell);

                }

                Row extractedRow = new Row(rowCells);
                extractedRows.add(extractedRow);
            }
            return extractedRows;
        }
        else {

            List<Row> rows = new ArrayList();

            for (Serializable[] bcRow: ((WriteRowsEventData) binlogConnectorEvent.getData()).getRows()) {

                for (int columnIndex = 0; columnIndex < bcRow.length; columnIndex++) {

                    Serializable cell = bcRow[columnIndex];

                    // Cell can have one of the following types:
                    //
                    //  Integer
                    //  Long
                    //  Float
                    //  Double
                    //  java.util.BitSet
                    //  java.util.Date
                    //  java.math.BigDecimal
                    //  java.sql.Timestamp
                    //  java.sql.Date
                    //  java.sql.Time
                    //  String
                    //  byte[]
                    //
                    // More details at: https://github.com/shyiko/mysql-binlog-connector-java/blob/3709c9668ffc732e053e0f93ca3a3610789b152c/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/AbstractRowsEventDataDeserializer.java
                    if(cell instanceof Integer){

                    }
                    if(cell instanceof Long){

                    }
                    if(cell instanceof Float){

                    }
                    if(cell instanceof Double){

                    }
                    if(cell instanceof java.util.BitSet){

                    }
                    if(cell instanceof java.util.Date){

                    }
                    if(cell instanceof java.math.BigDecimal){
                        BigDecimal bd = (BigDecimal)cell;
                        // TODO: create BigDecimal class
                    }
                    if(cell instanceof java.sql.Timestamp){

                    }
                    if(cell instanceof java.sql.Date){

                    }
                    if(cell instanceof java.sql.Time){

                    }
                    if(cell instanceof String){

                    }
                    if(cell instanceof byte[]){

                    }
                }
            }
            return rows;
        }
    }
}
