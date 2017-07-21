package com.booking.replication.binlog.event;

import com.booking.replication.binlog.common.Cell;
import com.booking.replication.binlog.common.CellExtractor;
import com.booking.replication.binlog.common.Row;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.code.or.binlog.impl.event.DeleteRowsEvent;
import com.google.code.or.binlog.impl.event.DeleteRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.common.glossary.Column;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Created by bosko on 6/1/17.
 */
public class RawBinlogEvent_Delete extends RawBinlogEvent_Rows {

    List<Row> extractedRows;

    public RawBinlogEvent_Delete(Object event) throws Exception {
        super(event);
        extractedRows = this.extractRowsFromEvent();
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

    public List<Row> getExtractedRows() {
        return extractedRows;
    }

    private List<Row> extractRowsFromEvent() throws Exception {

        List<Row> rows = new ArrayList();

        if (this.USING_DEPRECATED_PARSER) {
            for (com.google.code.or.common.glossary.Row orRow : ((DeleteRowsEventV2) binlogEventV4).getRows()) {
                List<Cell> cells = new ArrayList<>();
                for (Column column: orRow.getColumns()) {
                    Cell cell = CellExtractor.extractCellFromOpenReplicatorColumn(column);
                    cells.add(cell);
                }
                Row row = new Row(cells);
                rows.add(row);
            }
            return rows;
        }
        else {
            for (Serializable[] bcRow: ((DeleteRowsEventData) binlogConnectorEvent.getData()).getRows()) {
                List<Cell> cells = new ArrayList<>();
                for (int columnIndex = 0; columnIndex < bcRow.length; columnIndex++) {
                    Cell cell = CellExtractor.extractCellFromBinlogConnectorColumn(bcRow[columnIndex]);
                    cells.add(cell);
                }
                Row row = new Row(cells);
                rows.add(row);
            }
            return rows;
        }
    }
}
