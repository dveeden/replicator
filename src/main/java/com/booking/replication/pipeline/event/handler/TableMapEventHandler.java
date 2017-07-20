package com.booking.replication.pipeline.event.handler;

import com.booking.replication.Constants;
import com.booking.replication.Metrics;
import com.booking.replication.applier.Applier;
import com.booking.replication.pipeline.CurrentTransactionMetadata;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.pipeline.PipelinePosition;
import com.booking.replication.replicant.ReplicantPool;
import com.booking.replication.schema.exception.TableMapException;
import com.codahale.metrics.Meter;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.TableMapEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by edmitriev on 7/13/17.
 */
public class TableMapEventHandler implements BinlogEventV4Handler {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableMapEventHandler.class);

    private final EventHandlerConfiguration eventHandlerConfiguration;
    private final Meter heartBeatCounter = Metrics.registry.meter(name("events", "heartBeatCounter"));;
    private final PipelineOrchestrator pipelineOrchestrator;
    private final PipelinePosition pipelinePosition;
    private final ReplicantPool replicantPool;


    public TableMapEventHandler(EventHandlerConfiguration eventHandlerConfiguration,
                                PipelinePosition pipelinePosition, ReplicantPool replicantPool) {
        this.eventHandlerConfiguration = eventHandlerConfiguration;
        this.pipelinePosition = pipelinePosition;
        this.replicantPool = replicantPool;
        this.pipelineOrchestrator = eventHandlerConfiguration.getPipelineOrchestrator();
    }

    @Override
    public void apply(BinlogEventV4 binlogEventV4, CurrentTransactionMetadata currentTransactionMetadata) throws EventHandlerApplyException, TableMapException {
        final TableMapEvent event = (TableMapEvent) binlogEventV4;
        String tableName = event.getTableName().toString();

        if (tableName.equals(Constants.HEART_BEAT_TABLE)) {
            heartBeatCounter.mark();
        }

        long tableID = event.getTableId();
        String dbName = pipelineOrchestrator.currentTransactionMetadata.getDBNameFromTableID(tableID);

        LOGGER.debug("processing events for { db => " + dbName + " table => " + event.getTableName() + " } ");
        LOGGER.debug("fakeMicrosecondCounter at tableMap event => " + pipelineOrchestrator.getFakeMicrosecondCounter());

        eventHandlerConfiguration.getApplier().applyTableMapEvent(event);

        pipelinePosition.updatePipelineLastMapEventPosition(
                replicantPool.getReplicantDBActiveHost(),
                replicantPool.getReplicantDBActiveHostServerID(),
                event,
                pipelineOrchestrator.getFakeMicrosecondCounter()
        );
    }

    @Override
    public void handle(BinlogEventV4 binlogEventV4) throws TransactionException {
        final TableMapEvent event = (TableMapEvent) binlogEventV4;
        pipelineOrchestrator.currentTransactionMetadata.updateCache(event);
        pipelineOrchestrator.addEventIntoTransaction(event);
    }
}
