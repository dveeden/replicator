package com.booking.replication.binlog.event;

import com.booking.replication.binlog.common.Cell;
import com.booking.replication.binlog.common.CellExtractor;
import com.booking.replication.binlog.common.Row;
import com.booking.replication.binlog.common.RowPair;
import com.booking.replication.schema.column.ColumnSchema;
import com.booking.replication.schema.column.types.Converter;
import com.booking.replication.schema.exception.TableMapException;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.code.or.binlog.impl.event.UpdateRowsEventV2;
import com.google.code.or.binlog.impl.event.WriteRowsEvent;
import com.google.code.or.binlog.impl.event.WriteRowsEventV2;
import com.google.code.or.common.glossary.Column;
import com.google.code.or.common.glossary.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Created by bosko on 6/1/17.
 */
public class RawBinlogEvent_Update extends RawBinlogEvent_Rows {

    List<RowPair> extractedRows;

    public RawBinlogEvent_Update(Object event) throws Exception {
        super(event);
        this.extractedRows = this.extractRowsFromEvent();
    }

    public int getColumnCount() {
        if (this.binlogEventV4 != null) {
            return ((WriteRowsEvent) binlogEventV4).getColumnCount().intValue();
        }
        else {
            BitSet includedColumns = ((UpdateRowsEventData) binlogConnectorEvent.getData()).getIncludedColumns();
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

    public List<RowPair> getExtractedRows() {
        return extractedRows;
    }

    private List<RowPair> extractRowsFromEvent() throws Exception {

        if (this.USING_DEPRECATED_PARSER) {

            List<RowPair> pairs = new ArrayList<>();

            for (com.google.code.or.common.glossary.Pair rowPair : ((UpdateRowsEventV2) binlogEventV4).getRows()) {

                int numberOfColumns = getColumnCount();

                List<Cell> cellsBefore = new ArrayList<>();
                List<Cell> cellsAfter = new ArrayList<>();

                for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {

                    // but here index goes from 0..
                    Column columnValueBefore = ((com.google.code.or.common.glossary.Row) rowPair.getBefore()).getColumns().get(columnIndex);
                    Column columnValueAfter = ((com.google.code.or.common.glossary.Row) rowPair.getAfter()).getColumns().get(columnIndex);

                    Cell cellBefore = CellExtractor.extractCellFromOpenReplicatorColumn(columnValueBefore);
                    Cell cellAfter = CellExtractor.extractCellFromOpenReplicatorColumn(columnValueAfter);

                    cellsBefore.add(cellBefore);
                    cellsAfter.add(cellAfter);
                }

                Row rowBefore = new Row(cellsBefore);
                Row rowAfter = new Row(cellsAfter);

                pairs.add(new RowPair(rowBefore, rowAfter));
            }
            return pairs;
        }
        else {
            // TODO: extract RowPairs from binlog connector row format

            // we have a List<Map.Entry<Serializable[], Serializable[]>>
            // where one row is Map.Entry<Serializable[], Serializable[]>
            // TODO: verify:
            //      ? column ordering ?
            //      ? value overlap ?

            List<RowPair> pairs = new ArrayList<>();

            for (Map.Entry<Serializable[], Serializable[]> bcRowUpdateEntry: ((UpdateRowsEventData) binlogConnectorEvent.getData()).getRows()) {

                int numberOfColumns = getColumnCount();

                // TODO: verify that before values are on the key and after on the value
                Serializable[] bcRowBefore = bcRowUpdateEntry.getKey();
                Serializable[] bcRowAfter = bcRowUpdateEntry.getValue();

                List<Cell> cellsBefore = new ArrayList<>();
                List<Cell> cellsAfter = new ArrayList<>();

                for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
                    Cell cellBefore = CellExtractor.extractCellFromBinlogConnectorColumn(bcRowBefore[columnIndex]);
                    Cell cellAfter = CellExtractor.extractCellFromBinlogConnectorColumn(bcRowAfter[columnIndex]);
                    cellsBefore.add(cellBefore);
                    cellsAfter.add(cellAfter);
                }
                Row rowBefore = new Row(cellsBefore);
                Row rowAfter = new Row(cellsAfter);

                pairs.add(new RowPair(rowBefore, rowAfter));

            }
            return pairs;
        }
    }
}
