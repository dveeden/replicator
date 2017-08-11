package com.booking.replication.applier;

import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.pipeline.CurrentTransaction;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.PipelinePosition;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import com.google.code.or.binlog.impl.event.XidEvent;

import java.io.IOException;
import java.nio.channels.Pipe;

/**
 * Created by bosko on 11/14/15.
 */
public interface Applier {

    void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, CurrentTransaction currentTransaction, PipelinePosition pipelinePosition)
            throws ApplierException, IOException;

    void applyBeginQueryEvent(QueryEvent event, CurrentTransaction currentTransaction, PipelinePosition pipelinePosition);

    void applyCommitQueryEvent(QueryEvent event, CurrentTransaction currentTransaction, PipelinePosition pipelinePosition);

    void applyXidEvent(XidEvent event, CurrentTransaction currentTransaction, PipelinePosition pipelinePosition);

    void applyRotateEvent(RotateEvent event) throws ApplierException, IOException;

    void applyAugmentedSchemaChangeEvent(
            AugmentedSchemaChangeEvent augmentedSchemaChangeEvent,
            PipelineOrchestrator caller);

    void forceFlush() throws ApplierException, IOException;

    void applyFormatDescriptionEvent(FormatDescriptionEvent event);

    void applyTableMapEvent(TableMapEvent event);

    void waitUntilAllRowsAreCommitted(BinlogEventV4 event) throws IOException, ApplierException;

}
