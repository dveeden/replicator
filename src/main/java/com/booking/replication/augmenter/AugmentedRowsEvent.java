package com.booking.replication.augmenter;

import com.booking.replication.binlog.event.RawBinlogEvent_Write;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by bdevetak on 20/11/15.
 */
public class AugmentedRowsEvent {

    AugmentedRowsEvent(RawBinlogEvent_Write ev) {
        binlogFileName = ev.getBinlogFilename();
    }

    private String mysqlTableName;

    private List<AugmentedRow> singleRowEvents = new ArrayList<>();

    private String binlogFileName;

    public void addSingleRowEvent(AugmentedRow au) {
        singleRowEvents.add(au);
    }

    public List<AugmentedRow> getSingleRowEvents() {
        return singleRowEvents;
    }

    public String getMysqlTableName() {
        return mysqlTableName;
    }

    public void setMysqlTableName(String mysqlTableName) {
        this.mysqlTableName = mysqlTableName;
    }

    public String getBinlogFileName() {
        return binlogFileName;
    }

    public void setBinlogFileName(String binlogFileName) {
        this.binlogFileName = binlogFileName;
    }
}
