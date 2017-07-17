package com.booking.replication.pipeline.event.handler;

import com.booking.replication.Coordinator;
import com.booking.replication.applier.Applier;
import com.booking.replication.applier.ApplierException;
import com.booking.replication.applier.HBaseApplier;
import com.booking.replication.applier.hbase.TaskBufferInconsistencyException;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.augmenter.EventAugmenter;
import com.booking.replication.checkpoints.LastCommittedPositionCheckpoint;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.PipelinePosition;
import com.booking.replication.schema.ActiveSchemaVersion;
import com.booking.replication.schema.exception.SchemaTransitionException;
import com.booking.replication.sql.QueryInspector;
import com.booking.replication.sql.exception.QueryInspectorException;
import com.codahale.metrics.Meter;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by edmitriev on 7/12/17.
 */
public class QueryEventHandler implements AbstractHandler<QueryEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryEventHandler.class);
    private static EventAugmenter eventAugmenter;

    private final ActiveSchemaVersion activeSchemaVersion;
    private final Applier applier;
    private final Meter pgtidCounter;
    private final Meter commitQueryCounter;
    private final PipelineOrchestrator pipelineOrchestrator;
    private final PipelinePosition pipelinePosition;
    private final QueryInspector queryInspector;


    public QueryEventHandler(PipelineOrchestrator pipelineOrchestrator, ActiveSchemaVersion activeSchemaVersion,
                             Applier applier, Meter pgtidCounter, Meter commitQueryCounter,
                             PipelinePosition pipelinePosition, QueryInspector queryInspector,
                             EventAugmenter eventAugmenter) {
        this.activeSchemaVersion = activeSchemaVersion;
        this.applier = applier;
        this.pgtidCounter = pgtidCounter;
        this.commitQueryCounter = commitQueryCounter;
        this.pipelineOrchestrator = pipelineOrchestrator;
        this.pipelinePosition = pipelinePosition;
        this.queryInspector = queryInspector;
        this.eventAugmenter = eventAugmenter;
    }

    @Override
    public void apply(QueryEvent event, long xid) throws EventHandlerApplyException, ApplierException, IOException {
        String querySQL = event.getSql().toString();

        switch (queryInspector.getQueryEventType(event)) {
            case "COMMIT":
                commitQueryCounter.mark();
                applier.applyCommitQueryEvent(event);
                break;
            case "BEGIN":
                applier.applyBeginQueryEvent(event);
                break;
            case "DDLTABLE":
                // Sync all the things here.
                applier.forceFlush();
                applier.waitUntilAllRowsAreCommitted(event);

                try {
                    AugmentedSchemaChangeEvent augmentedSchemaChangeEvent = activeSchemaVersion.transitionSchemaToNextVersion(
                            eventAugmenter.getSchemaTransitionSequence(event),
                            event.getHeader().getTimestamp()
                    );

                    String currentBinlogFileName =
                            pipelinePosition.getCurrentPosition().getBinlogFilename();

                    long currentBinlogPosition = event.getHeader().getPosition();

                    String pseudoGTID = pipelinePosition.getCurrentPseudoGTID();
                    String pseudoGTIDFullQuery = pipelinePosition.getCurrentPseudoGTIDFullQuery();
                    int currentSlaveId = pipelinePosition.getCurrentPosition().getServerID();

                    LastCommittedPositionCheckpoint marker = new LastCommittedPositionCheckpoint(
                            pipelinePosition.getCurrentPosition().getHost(),
                            currentSlaveId,
                            currentBinlogFileName,
                            currentBinlogPosition,
                            pseudoGTID,
                            pseudoGTIDFullQuery,
                            pipelineOrchestrator.getFakeMicrosecondCounter()
                    );

                    LOGGER.info("Save new marker: " + marker.toJson());
                    Coordinator.saveCheckpointMarker(marker);
                    applier.applyAugmentedSchemaChangeEvent(augmentedSchemaChangeEvent, pipelineOrchestrator);
                } catch (SchemaTransitionException e) {
                    LOGGER.error("Failed to apply query", e);
                    throw new EventHandlerApplyException("Failed to apply event", e);
                } catch (Exception e) {
                    throw new EventHandlerApplyException("Failed to apply event", e);
                }
                break;
            case "DDLVIEW":
                // TODO: add view schema changes to view schema history
                break;
            case  "PSEUDOGTID":
                pgtidCounter.mark();

                try {
                    String pseudoGTID = queryInspector.extractPseudoGTID(querySQL);

                    pipelinePosition.setCurrentPseudoGTID(pseudoGTID);
                    pipelinePosition.setCurrentPseudoGTIDFullQuery(querySQL);
                    if (applier instanceof HBaseApplier) {
                        try {
                            ((HBaseApplier) applier).applyPseudoGTIDEvent(new LastCommittedPositionCheckpoint(
                                    pipelinePosition.getCurrentPosition().getHost(),
                                    pipelinePosition.getCurrentPosition().getServerID(),
                                    pipelinePosition.getCurrentPosition().getBinlogFilename(),
                                    pipelinePosition.getCurrentPosition().getBinlogPosition(),
                                    pseudoGTID,
                                    querySQL,
                                    pipelineOrchestrator.getFakeMicrosecondCounter()
                            ));
                        } catch (TaskBufferInconsistencyException e) {
                            e.printStackTrace();
                        }
                    }
                } catch (QueryInspectorException e) {
                    LOGGER.error("Failed to update pipelinePosition with new pGTID!", e);
                    throw new EventHandlerApplyException("Failed to apply event", e);
                }
                break;
            default:
                LOGGER.warn("Unexpected query event: " + querySQL);
                break;
        }
    }

    @Override
    public void handle(QueryEvent event) throws TransactionException {
        switch (queryInspector.getQueryEventType(event)) {
            case "COMMIT":
                pipelineOrchestrator.addEventIntoTransaction(event);
                pipelineOrchestrator.commitTransaction(event);
                break;
            case "BEGIN":
                if (!pipelineOrchestrator.beginTransaction()) {
                    throw new TransactionException("Failed to begin new transaction. Already have one: " + pipelineOrchestrator.getCurrentTransactionMetadata());
                }
                pipelineOrchestrator.addEventIntoTransaction(event);
                break;
            case "DDLTABLE":
            case "DDLVIEW":
                pipelineOrchestrator.addEventIntoTransaction(event);
                break;
            case "PSEUDOGTID":
                // apply event right away through a fake transaction
                if (!pipelineOrchestrator.beginTransaction()) {
                    throw new TransactionException("Failed to begin new transaction. Already have one: " + pipelineOrchestrator.getCurrentTransactionMetadata());
                }
                pipelineOrchestrator.addEventIntoTransaction(event);
                pipelineOrchestrator.commitTransaction(event.getHeader().getTimestamp(), 0);
                break;
            default:
                LOGGER.warn("Unexpected query event: " + event.getSql());
                pipelineOrchestrator.addEventIntoTransaction(event);
        }
    }
}
