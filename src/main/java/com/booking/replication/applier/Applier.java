package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.binlog.event.RawBinlogEventFormatDescription;
import com.booking.replication.binlog.event.RawBinlogEventRotate;
import com.booking.replication.binlog.event.RawBinlogEventTableMap;
import com.booking.replication.binlog.event.RawBinlogEventXid;
import com.booking.replication.pipeline.PipelineOrchestrator;

import java.io.IOException;

/**
 * Created by bosko on 11/14/15.
 */
public interface Applier {

    void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, PipelineOrchestrator caller)
            throws ApplierException, IOException;

    void applyCommitQueryEvent();

    void applyXidEvent(RawBinlogEventXid event);

    void applyRotateEvent(RawBinlogEventRotate event) throws ApplierException, IOException;

    void applyAugmentedSchemaChangeEvent(
            AugmentedSchemaChangeEvent augmentedSchemaChangeEvent,
            PipelineOrchestrator caller);

    void forceFlush() throws ApplierException, IOException;

    void applyFormatDescriptionEvent(RawBinlogEventFormatDescription event);

    void applyTableMapEvent(RawBinlogEventTableMap event);

    void waitUntilAllRowsAreCommitted() throws IOException, ApplierException;

}
