package com.booking.replication.pipeline.event.handler;

import com.booking.replication.Coordinator;
import com.booking.replication.applier.Applier;
import com.booking.replication.applier.ApplierException;
import com.booking.replication.checkpoints.LastCommittedPositionCheckpoint;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.PipelinePosition;
import com.codahale.metrics.Meter;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by edmitriev on 7/12/17.
 */
public class RotateEventHandler implements AbstractHandler<RotateEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RotateEventHandler.class);

    private final Applier applier;
    private final PipelineOrchestrator pipelineOrchestrator;
    private final PipelinePosition pipelinePosition;
    private final String lastBinlogFileName;


    public RotateEventHandler(PipelineOrchestrator pipelineOrchestrator, Applier applier, PipelinePosition pipelinePosition, String lastBinlogFileName) {
        this.pipelineOrchestrator = pipelineOrchestrator;
        this.applier = applier;
        this.pipelinePosition = pipelinePosition;
        this.lastBinlogFileName = lastBinlogFileName;
    }

    @Override
    public void apply(RotateEvent event, long xid) throws EventHandlerApplyException, ApplierException, IOException {
        try {
            applier.applyRotateEvent(event);
        } catch (IOException e) {
            throw new EventHandlerApplyException("Failed to apply event", e);
        }
        LOGGER.info("End of binlog file. Waiting for all tasks to finish before moving forward...");

        //TODO: Investigate if this is the right thing to do.

        applier.waitUntilAllRowsAreCommitted(event);


        String currentBinlogFileName =
                pipelinePosition.getCurrentPosition().getBinlogFilename();

        String nextBinlogFileName = event.getBinlogFileName().toString();
        long currentBinlogPosition = event.getBinlogPosition();

        LOGGER.info("All rows committed for binlog file "
                + currentBinlogFileName + ", moving to next binlog " + nextBinlogFileName);

        String pseudoGTID = pipelinePosition.getCurrentPseudoGTID();
        String pseudoGTIDFullQuery = pipelinePosition.getCurrentPseudoGTIDFullQuery();
        int currentSlaveId = pipelinePosition.getCurrentPosition().getServerID();

        LastCommittedPositionCheckpoint marker = new LastCommittedPositionCheckpoint(
                pipelinePosition.getCurrentPosition().getHost(),
                currentSlaveId,
                nextBinlogFileName,
                currentBinlogPosition,
                pseudoGTID,
                pseudoGTIDFullQuery,
                pipelineOrchestrator.getFakeMicrosecondCounter()
        );

        try {
            Coordinator.saveCheckpointMarker(marker);
        } catch (Exception e) {
            LOGGER.error("Failed to save Checkpoint!");
            e.printStackTrace();
        }

        if (currentBinlogFileName.equals(lastBinlogFileName)) {
            LOGGER.info("processed the last binlog file " + lastBinlogFileName);
            pipelineOrchestrator.requestShutdown();
        }
    }

    @Override
    public void handle(RotateEvent event) throws TransactionException {
        pipelineOrchestrator.addEventIntoTransaction(event);
    }
}
