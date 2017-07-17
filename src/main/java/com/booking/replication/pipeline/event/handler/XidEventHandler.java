package com.booking.replication.pipeline.event.handler;

import com.booking.replication.applier.Applier;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.codahale.metrics.Meter;
import com.google.code.or.binlog.impl.event.XidEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by edmitriev on 7/12/17.
 */
public class XidEventHandler implements AbstractHandler<XidEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(XidEventHandler.class);

    private final Applier applier;
    private final Meter counter;
    private final PipelineOrchestrator pipelineOrchestrator;


    public XidEventHandler(PipelineOrchestrator pipelineOrchestrator, Applier applier, Meter counter) {
        this.pipelineOrchestrator = pipelineOrchestrator;
        this.applier = applier;
        this.counter = counter;
    }


    @Override
    public void apply(XidEvent event, long xid) throws EventHandlerApplyException {
        if (xid != event.getXid()) {
            throw new EventHandlerApplyException("Xid of transaction doesn't match the current event xid: " + xid + ", " + event);
        }
        applier.applyXidEvent(event);
        counter.mark();
    }

    @Override
    public void handle(XidEvent event) throws TransactionException {
        // prepare trans data
        pipelineOrchestrator.addEventIntoTransaction(event);
        pipelineOrchestrator.commitTransaction(event);
    }
}
