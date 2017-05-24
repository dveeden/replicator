package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.binlog.RawBinlogEvent_FormatDescription;
import com.booking.replication.binlog.RawBinlogEvent_Rotate;
import com.booking.replication.binlog.RawBinlogEvent_TableMap;
import com.booking.replication.binlog.RawBinlogEvent_Xid;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.codahale.metrics.Counter;

import java.io.IOException;

/**
 * Wraps an applier to count incoming events
 */
public class EventCountingApplier implements Applier {

    private final Applier wrapped;
    private final Counter counter;

    public EventCountingApplier(Applier wrapped, Counter counter)
        {
            if (wrapped == null)
            {
                throw new IllegalArgumentException("wrapped must not be null");
            }

            if (counter == null)
            {
                throw new IllegalArgumentException("counter must not be null");
            }

            this.wrapped = wrapped;
            this.counter = counter;
        }

    @Override
    public void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, PipelineOrchestrator caller) throws ApplierException, IOException {
        wrapped.applyAugmentedRowsEvent(augmentedSingleRowEvent, caller);
        counter.inc();
    }

    @Override
    public void applyCommitQueryEvent() {
        wrapped.applyCommitQueryEvent();
        counter.inc();
    }

    @Override
    public void applyXidEvent(RawBinlogEvent_Xid event) {
        wrapped.applyXidEvent(event);
        counter.inc();
    }

    @Override
    public void applyRotateEvent(RawBinlogEvent_Rotate event) throws ApplierException, IOException {
        wrapped.applyRotateEvent(event);
    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) {
        wrapped.applyAugmentedSchemaChangeEvent(augmentedSchemaChangeEvent, caller);
        counter.inc();
    }

    @Override
    public void forceFlush() throws ApplierException, IOException {
        wrapped.forceFlush();
    }

    @Override
    public void applyFormatDescriptionEvent(RawBinlogEvent_FormatDescription event) {
        wrapped.applyFormatDescriptionEvent(event);
        counter.inc();
    }

    @Override
    public void applyTableMapEvent(RawBinlogEvent_TableMap event) {
        wrapped.applyTableMapEvent(event);
        counter.inc();
    }

    @Override
    public void waitUntilAllRowsAreCommitted() throws IOException, ApplierException {
        wrapped.waitUntilAllRowsAreCommitted();
    }
}
