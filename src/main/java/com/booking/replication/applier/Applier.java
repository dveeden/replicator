package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.binlog.RawBinlogEvent_FormatDescription;
import com.booking.replication.binlog.RawBinlogEvent_Rotate;
import com.booking.replication.binlog.RawBinlogEvent_TableMap;
import com.booking.replication.binlog.RawBinlogEvent_Xid;
import com.booking.replication.pipeline.PipelineOrchestrator;

import java.io.IOException;

/**
 * Created by bosko on 11/14/15.
 */
public interface Applier {

    void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, PipelineOrchestrator caller)
            throws ApplierException, IOException;

    void applyCommitQueryEvent();

    void applyXidEvent(RawBinlogEvent_Xid event);

    void applyRotateEvent(RawBinlogEvent_Rotate event) throws ApplierException, IOException;

    void applyAugmentedSchemaChangeEvent(
            AugmentedSchemaChangeEvent augmentedSchemaChangeEvent,
            PipelineOrchestrator caller);

    void forceFlush() throws ApplierException, IOException;

    void applyFormatDescriptionEvent(RawBinlogEvent_FormatDescription event);

    void applyTableMapEvent(RawBinlogEvent_TableMap event);

    void waitUntilAllRowsAreCommitted() throws IOException, ApplierException;

}
