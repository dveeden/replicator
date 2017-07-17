package com.booking.replication.pipeline.event.handler;

import com.booking.replication.Constants;
import com.booking.replication.applier.Applier;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.PipelinePosition;
import com.booking.replication.replicant.ReplicantPool;
import com.booking.replication.schema.exception.TableMapException;
import com.codahale.metrics.Meter;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by edmitriev on 7/13/17.
 */
public class TableMapEventHandler implements AbstractHandler<TableMapEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableMapEventHandler.class);

    private final Applier applier;
    private final Meter heartBeatCounter;
    private final PipelineOrchestrator pipelineOrchestrator;
    private final PipelinePosition pipelinePosition;
    private final ReplicantPool replicantPool;


    public TableMapEventHandler(PipelineOrchestrator pipelineOrchestrator, Applier applier, Meter heartBeatCounter,
                                PipelinePosition pipelinePosition, ReplicantPool replicantPool) {
        this.applier = applier;
        this.heartBeatCounter = heartBeatCounter;
        this.pipelineOrchestrator = pipelineOrchestrator;
        this.pipelinePosition = pipelinePosition;
        this.replicantPool = replicantPool;
    }

    @Override
    public void apply(TableMapEvent event, long xid) throws EventHandlerApplyException, TableMapException {
        String tableName = event.getTableName().toString();

        if (tableName.equals(Constants.HEART_BEAT_TABLE)) {
            heartBeatCounter.mark();
        }

        long tableID = event.getTableId();
        String dbName = pipelineOrchestrator.currentTransactionMetadata.getDBNameFromTableID(tableID);

        LOGGER.debug("processing events for { db => " + dbName + " table => " + event.getTableName() + " } ");
        LOGGER.debug("fakeMicrosecondCounter at tableMap event => " + pipelineOrchestrator.getFakeMicrosecondCounter());

        applier.applyTableMapEvent(event);

        pipelinePosition.updatePipelineLastMapEventPosition(
                replicantPool.getReplicantDBActiveHost(),
                replicantPool.getReplicantDBActiveHostServerID(),
                event,
                pipelineOrchestrator.getFakeMicrosecondCounter()
        );
    }

    @Override
    public void handle(TableMapEvent event) throws TransactionException {
        pipelineOrchestrator.currentTransactionMetadata.updateCache(event);
        pipelineOrchestrator.addEventIntoTransaction(event);
    }
}
