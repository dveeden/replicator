package com.booking.replication.pipeline.event.handler;

import com.booking.replication.applier.Applier;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.codahale.metrics.Meter;
import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by edmitriev on 7/12/17.
 */
public class FormatDescriptionEventHandler implements AbstractHandler<FormatDescriptionEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FormatDescriptionEventHandler.class);

    private final Applier applier;
    private final PipelineOrchestrator pipelineOrchestrator;

    public FormatDescriptionEventHandler(PipelineOrchestrator pipelineOrchestrator, Applier applier) {
        this.pipelineOrchestrator = pipelineOrchestrator;
        this.applier = applier;
    }


    @Override
    public void apply(FormatDescriptionEvent event, long xid) {
        applier.applyFormatDescriptionEvent(event);
    }

    @Override
    public void handle(FormatDescriptionEvent event) throws TransactionException {
        pipelineOrchestrator.addEventIntoTransaction(event);
    }
}
