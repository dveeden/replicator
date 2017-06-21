package com.booking.replication.binlog.common;

import com.google.common.base.Joiner;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by bosko on 6/21/17.
 *
 * ParsedRow is just a list of ParsedColumns.
 */
public class ExtractedRow {

    private List<ExtractedColumn> extractedColumns;

    /**
     *
     */
    public ExtractedRow() {
    }

    public ExtractedRow(List<ExtractedColumn> extractedColumns) {
        this.extractedColumns = extractedColumns;
    }

    /**
     *
     */
    @Override
    public String toString() {
        List<String> cl = extractedColumns.stream()
                                 .map(c -> c.getValue().toString())
                                 .collect(Collectors.toList());
        return "parsedColumns: " + Joiner.on(",").join(cl);
    }

    /**
     *
     */
    public List<ExtractedColumn> getExtractedColumns() {
        return extractedColumns;
    }

    public void setExtractedColumns(List<ExtractedColumn> extractedColumns) {
        this.extractedColumns = extractedColumns;
    }
}
