package com.booking.replication.pipeline.event.handler;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.ApplierException;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.EventAugmenter;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.schema.exception.TableMapException;
import com.codahale.metrics.Meter;
import com.google.code.or.binlog.impl.event.AbstractRowEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by edmitriev on 7/12/17.
 */
public class UpdateRowsEventHandler implements AbstractHandler<AbstractRowEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateRowsEventHandler.class);
    private final Applier applier;
    private final EventAugmenter eventAugmenter;
    private final Meter updateEventCounter;
    private final PipelineOrchestrator pipelineOrchestrator;


    public UpdateRowsEventHandler(PipelineOrchestrator pipelineOrchestrator, Applier applier, Meter updateEventCounter, EventAugmenter eventAugmenter) {
        this.applier = applier;
        this.updateEventCounter = updateEventCounter;
        this.eventAugmenter = eventAugmenter;
        this.pipelineOrchestrator = pipelineOrchestrator;
    }

    @Override
    public void apply(AbstractRowEvent event, long xid) throws TableMapException, ApplierException, IOException {
        AugmentedRowsEvent augmentedRowsEvent = eventAugmenter.mapDataEventToSchema(event, pipelineOrchestrator);
        applier.applyAugmentedRowsEvent(augmentedRowsEvent, pipelineOrchestrator);
        updateEventCounter.mark();
    }

    @Override
    public void handle(AbstractRowEvent event) throws TransactionException {
        pipelineOrchestrator.addEventIntoTransaction(event);
    }
}
