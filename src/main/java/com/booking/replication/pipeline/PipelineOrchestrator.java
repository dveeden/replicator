package com.booking.replication.pipeline;

import com.booking.replication.Configuration;
import com.booking.replication.Coordinator;
import com.booking.replication.Metrics;
import com.booking.replication.applier.Applier;
import com.booking.replication.applier.ApplierException;
import com.booking.replication.applier.HBaseApplier;
import com.booking.replication.augmenter.EventAugmenter;
import com.booking.replication.binlog.EventPosition;
import com.booking.replication.checkpoints.LastCommittedPositionCheckpoint;
import com.booking.replication.pipeline.event.handler.*;
import com.booking.replication.queues.ReplicatorQueues;
import com.booking.replication.replicant.ReplicantPool;
import com.booking.replication.schema.ActiveSchemaVersion;
import com.booking.replication.schema.exception.SchemaTransitionException;
import com.booking.replication.schema.exception.TableMapException;
import com.booking.replication.sql.QueryInspector;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.code.or.binlog.BinlogEventV4;
import com.google.code.or.binlog.impl.event.*;
import com.google.code.or.common.util.MySQLConstants;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;


/**
 * Pipeline Orchestrator.
 *
 * <p>Manages data flow from event producer into the applier.
 * Also manages persistence of metadata necessary for the replicator features.</p>
 *
 * <p>On each event handles:
 *      1. schema version management
 *      2  augmenting events with schema info
 *      3. sending of events to applier.
 * </p>
 */
public class PipelineOrchestrator extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineOrchestrator.class);
    private static final Meter eventsReceivedCounter = Metrics.registry.meter(name("events", "eventsReceivedCounter"));
    private static final Meter eventsProcessedCounter = Metrics.registry.meter(name("events", "eventsProcessedCounter"));
    private static final Meter eventsSkippedCounter = Metrics.registry.meter(name("events", "eventsSkippedCounter"));
    private static final Meter eventsRewindedCounter = Metrics.registry.meter(name("events", "eventsRewindedCounter"));

    private static final int BUFFER_FLUSH_INTERVAL = 30000; // <- force buffer flush every 30 sec
    private static final int DEFAULT_VERSIONS_FOR_MIRRORED_TABLES = 1000;
    private static final int TRANSACTION_SIZE_LIMIT = 500;
    private static EventAugmenter eventAugmenter;
    private static ActiveSchemaVersion activeSchemaVersion;
    private static LastCommittedPositionCheckpoint lastVerifiedPseudoGTIDCheckPoint;
    public final Configuration configuration;
    private final ReplicantPool replicantPool;
    private final Applier applier;
    private final ReplicatorQueues queues;
    private final EventDispatcher eventDispatcher = new EventDispatcher();
    ;
    private final PipelinePosition pipelinePosition;
    private final BinlogEventProducer binlogEventProducer;
    public CurrentTransactionMetadata currentTransactionMetadata;

    private volatile boolean running = false;
    private volatile boolean replicatorShutdownRequested = false;


    private HashMap<String, Boolean> rotateEventAllreadySeenForBinlogFile = new HashMap<>();

    /**
     * Fake microsecond counter.
     * <p>
     * <p>This is a special feature that
     * requires some explanation</p>
     * <p>
     * <p>MySQL binlog events have second-precision timestamps. This
     * obviously means that we can't have microsecond precision,
     * but that is not the intention here. The idea is to at least
     * preserve the information about ordering of events,
     * especially if one ID has multiple events within the same
     * second. We want to know what was their order. That is the
     * main purpose of this counter.</p>
     */
    private long fakeMicrosecondCounter = 0L;
    private long previousTimestamp = 0L;
    private long timeOfLastEvent = 0L;
    private boolean isRewinding = false;

    private Long replDelay = 0L;

    public PipelineOrchestrator(
            ReplicatorQueues repQueues,
            PipelinePosition pipelinePosition,
            Configuration repcfg,
            Applier applier,
            ReplicantPool replicantPool,
            BinlogEventProducer binlogEventProducer,
            long fakeMicrosecondCounter) throws SQLException, URISyntaxException {

        queues = repQueues;
        configuration = repcfg;

        this.replicantPool = replicantPool;
        this.fakeMicrosecondCounter = fakeMicrosecondCounter;
        this.binlogEventProducer = binlogEventProducer;

        activeSchemaVersion = new ActiveSchemaVersion(configuration);
        eventAugmenter = new EventAugmenter(activeSchemaVersion, configuration.getAugmenterApplyUuid(), configuration.getAugmenterApplyXid());

        currentTransactionMetadata = null;

        this.applier = applier;

        LOGGER.info("Created consumer with binlog position => { "
                + " binlogFileName => "
                + pipelinePosition.getCurrentPosition().getBinlogFilename()
                + ", binlogPosition => "
                + pipelinePosition.getCurrentPosition().getBinlogPosition()
                + " }"
        );

        Metrics.registry.register(MetricRegistry.name("events", "replicatorReplicationDelay"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return replDelay;
                    }
                });

        this.pipelinePosition = pipelinePosition;

        initEventDispatcher();
    }

    private void initEventDispatcher() {
        EventHandlerConfiguration eventHandlerConfiguration = new EventHandlerConfiguration(applier, eventAugmenter, this);

        eventDispatcher.registerHandler(
                MySQLConstants.QUERY_EVENT,
                new QueryEventHandler(eventHandlerConfiguration, activeSchemaVersion, pipelinePosition));

        eventDispatcher.registerHandler(
                MySQLConstants.TABLE_MAP_EVENT,
                new TableMapEventHandler(eventHandlerConfiguration, pipelinePosition, replicantPool));

        eventDispatcher.registerHandler(Arrays.asList(
                MySQLConstants.UPDATE_ROWS_EVENT, MySQLConstants.UPDATE_ROWS_EVENT_V2),
                new UpdateRowsEventHandler(eventHandlerConfiguration));

        eventDispatcher.registerHandler(Arrays.asList(
                MySQLConstants.WRITE_ROWS_EVENT, MySQLConstants.WRITE_ROWS_EVENT_V2),
                new WriteRowsEventHandler(eventHandlerConfiguration));

        eventDispatcher.registerHandler(Arrays.asList(
                MySQLConstants.DELETE_ROWS_EVENT, MySQLConstants.DELETE_ROWS_EVENT_V2),
                new DeleteRowsEventHandler(eventHandlerConfiguration));

        eventDispatcher.registerHandler(
                MySQLConstants.XID_EVENT,
                new XidEventHandler(eventHandlerConfiguration));

        eventDispatcher.registerHandler(
                MySQLConstants.FORMAT_DESCRIPTION_EVENT,
                new FormatDescriptionEventHandler(eventHandlerConfiguration));

        eventDispatcher.registerHandler(
                MySQLConstants.ROTATE_EVENT,
                new RotateEventHandler(eventHandlerConfiguration, pipelinePosition, configuration.getLastBinlogFileName()));

        eventDispatcher.registerHandler(
                MySQLConstants.STOP_EVENT,
                new DummyEventHandler());
    }

    public long getFakeMicrosecondCounter() {
        return fakeMicrosecondCounter;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public void requestReplicatorShutdown() {
        replicatorShutdownRequested = true;
    }

    public void requestShutdown() {
        setRunning(false);
        requestReplicatorShutdown();
    }

    public boolean isReplicatorShutdownRequested() {
        return replicatorShutdownRequested;
    }

    @Override
    public void run() {
        setRunning(true);
        timeOfLastEvent = System.currentTimeMillis();
        try {
            // block in a loop
            processQueueLoop();

        } catch (SchemaTransitionException e) {
            LOGGER.error("SchemaTransitionException, requesting replicator shutdown...", e);
            LOGGER.error("Original exception:", e.getOriginalException());
            requestReplicatorShutdown();
        } catch (InterruptedException e) {
            LOGGER.error("InterruptedException, requesting replicator shutdown...", e);
            requestReplicatorShutdown();
        } catch (TableMapException e) {
            LOGGER.error("TableMapException, requesting replicator shutdown...", e);
            requestReplicatorShutdown();
        } catch (IOException e) {
            LOGGER.error("IOException, requesting replicator shutdown...", e);
            requestReplicatorShutdown();
        } catch (Exception e) {
            LOGGER.error("Exception, requesting replicator shutdown...", e);
            requestReplicatorShutdown();
        }
    }


    private void processQueueLoop() throws Exception {
        while (isRunning()) {
            processQueue(null);
        }
    }

    private void processQueueLoop(BinlogPositionInfo exitOnBinlogPosition) throws Exception {
        while (isRunning()) {
            if (!processQueue(exitOnBinlogPosition)) {
                break;
            }
            if (isRewinding) {
                applyTransactionDataEvents();
            }
        }
    }

    private boolean processQueue(BinlogPositionInfo exitOnBinlogPosition) throws Exception {

        BinlogEventV4 event = waitForEvent();
        LOGGER.debug("Received event: " + event);

        timeOfLastEvent = System.currentTimeMillis();
        eventsReceivedCounter.mark();

        // Update pipeline position
        fakeMicrosecondCounter++;
        pipelinePosition.updateCurrentPipelinePosition(
                replicantPool.getReplicantDBActiveHost(),
                replicantPool.getReplicantDBActiveHostServerID(),
                event,
                fakeMicrosecondCounter
        );

        if (skipEvent(event)) {
            LOGGER.debug("Skipping event: " + event);
            eventsSkippedCounter.mark();
        } else {
            LOGGER.debug("Processing event: " + event);
            calculateAndPropagateChanges(event);
            eventsProcessedCounter.mark();
        }

        BinlogPositionInfo currentPosition = new BinlogPositionInfo(replicantPool.getReplicantDBActiveHostServerID(),
                EventPosition.getEventBinlogFileName(event), EventPosition.getEventBinlogPosition(event));
        LOGGER.info("exit:" + exitOnBinlogPosition);
        LOGGER.info("curr:" + currentPosition);
        return exitOnBinlogPosition == null || BinlogPositionInfo.compare(exitOnBinlogPosition, currentPosition) != 0;
    }

    private BinlogEventV4 rewindToEventType(String type) throws ApplierException, IOException, InterruptedException {
        // TODO: refactor
        LOGGER.debug("Rewinding to the event type: " + type);
        BinlogEventV4 resultEvent = null;
        while (isRunning()) {
            BinlogEventV4 event = waitForEvent();
            eventsRewindedCounter.mark();

            if (type.equals(event.getClass().toString())) {
                resultEvent = event;
                break;
            }

            LOGGER.debug("Skipping event due to rewinding: " + event);
        }
        LOGGER.debug("Rewinded to the position: " + EventPosition.getEventBinlogFileNameAndPosition(resultEvent) + ", event: " + resultEvent);
        return resultEvent;
    }

    private BinlogEventV4 waitForEvent() throws InterruptedException, ApplierException, IOException {
        while (isRunning()) {
            if (queues.rawQueue.size() > 0) {
                BinlogEventV4 event = queues.rawQueue.poll(100, TimeUnit.MILLISECONDS);

                if (event == null) {
                    LOGGER.warn("Poll timeout. Will sleep for 1s and try again.");
                    Thread.sleep(1000);
                    continue;
                }
                return event;

            } else {
                LOGGER.debug("Pipeline report: no items in producer event rawQueue. Will sleep for 0.5s and check again.");
                Thread.sleep(500);
                long currentTime = System.currentTimeMillis();
                long timeDiff = currentTime - timeOfLastEvent;
                boolean forceFlush = (timeDiff > BUFFER_FLUSH_INTERVAL);
                if (forceFlush) {
                    applier.forceFlush();
                }
            }
        }
        return null;
    }


    /**
     *  Calculate and propagate changes.
     *
     *  <p>STEPS:
     *     ======
     *  1. check event type
     *
     *  2. if DDL:
     *      a. pass to eventAugmenter which will update the schema
     *
     *  3. if DATA:
     *      a. match column names and types
     * </p>
     */
    public void calculateAndPropagateChanges(BinlogEventV4 event) throws Exception {

        if (fakeMicrosecondCounter > 999998L) {
            fakeMicrosecondCounter = 0L;
            LOGGER.warn("Fake microsecond counter's overflowed, resetting to 0. It might lead to incorrect events order.");
        }

        // Calculate replication delay before the event timestamp is extended with fake miscrosecond part
        // Note: there is a bug in open replicator which results in rotate event having timestamp value = 0.
        //       This messes up the replication delay time series. The workaround is not to calculate the
        //       replication delay at rotate event.
        if (event.getHeader() != null) {
            if ((event.getHeader().getTimestampOfReceipt() > 0)
                    && (event.getHeader().getTimestamp() > 0) ) {
                replDelay = event.getHeader().getTimestampOfReceipt() - event.getHeader().getTimestamp();
            } else {
                if (event.getHeader().getEventType() == MySQLConstants.ROTATE_EVENT) {
                    // do nothing, expected for rotate event
                } else {
                    // warn, not expected for other events
                    LOGGER.warn("Invalid timestamp value for event " + event.toString());
                }
            }
        } else {
            LOGGER.error("Event header can not be null. Shutting down...");
            requestReplicatorShutdown();
        }

        // check if the applier commit stream moved to a new check point. If so,
        // store the the new safe check point; currently only supported for hbase applier
        if (applier instanceof HBaseApplier) {
            LastCommittedPositionCheckpoint lastCommittedPseudoGTIDReportedByApplier =
                ((HBaseApplier) applier).getLastCommittedPseudGTIDCheckPoint();

            if (lastVerifiedPseudoGTIDCheckPoint == null
                    && lastCommittedPseudoGTIDReportedByApplier != null) {
                lastVerifiedPseudoGTIDCheckPoint = lastCommittedPseudoGTIDReportedByApplier;
                LOGGER.info("Save new marker: " + lastVerifiedPseudoGTIDCheckPoint.toJson());
                Coordinator.saveCheckpointMarker(lastVerifiedPseudoGTIDCheckPoint);
            } else if (lastVerifiedPseudoGTIDCheckPoint != null
                    && lastCommittedPseudoGTIDReportedByApplier != null) {
                if (!lastVerifiedPseudoGTIDCheckPoint.getPseudoGTID().equals(
                        lastCommittedPseudoGTIDReportedByApplier.getPseudoGTID())) {
                    LOGGER.info("Reached new safe checkpoint " + lastCommittedPseudoGTIDReportedByApplier.getPseudoGTID() );
                    lastVerifiedPseudoGTIDCheckPoint = lastCommittedPseudoGTIDReportedByApplier;
                    LOGGER.info("Save new marker: " + lastVerifiedPseudoGTIDCheckPoint.toJson());
                    Coordinator.saveCheckpointMarker(lastVerifiedPseudoGTIDCheckPoint);
                }
            }
        }

        long originalTimestamp = event.getHeader().getTimestamp();
        if (originalTimestamp > previousTimestamp) {
            fakeMicrosecondCounter = 0L;
            previousTimestamp = originalTimestamp;
        }

        // TODO: do we need it here with transactions?
        doTimestampOverride(event);

        // Process Event
        try {
            eventDispatcher.handle(event);
        } catch (TransactionSizeLimitException e) {
            LOGGER.info("Transaction size limit(" + TRANSACTION_SIZE_LIMIT + ") exceeded. Applying with rewinding xid: " + currentTransactionMetadata.getXid());
            applyTransactionWithRewinding(currentTransactionMetadata.getBeginEvent());
        } catch (TransactionException e) {
            LOGGER.error("EventManger failed to handle event: ", e);
            requestShutdown();
        }
    }

    public boolean isReplicant(String schemaName) {
        return schemaName.equals(configuration.getReplicantSchemaName());
    }

    /**
     * Returns true if event type is not tracked, or does not belong to the
     * tracked database.
     *
     * @param  event Binlog event that needs to be checked
     * @return shouldSkip Weather event should be skipped or processed
     */
    public boolean skipEvent(BinlogEventV4 event) throws Exception {
        // if there is a last safe checkpoint, skip events that are before
        // or equal to it, so that the same events are not writen multiple
        // times (beside wasting IO, this would fail the DDL operations,
        // for example trying to create a table that allready exists)
        if (pipelinePosition.getLastSafeCheckPointPosition() != null) {
            if ((pipelinePosition.getLastSafeCheckPointPosition().greaterThan(pipelinePosition.getCurrentPosition()))
                    || (pipelinePosition.getLastSafeCheckPointPosition().equals(pipelinePosition.getCurrentPosition()))) {
                LOGGER.info("Event position { binlog-filename => "
                        + pipelinePosition.getCurrentPosition().getBinlogFilename()
                        + ", binlog-position => "
                        + pipelinePosition.getCurrentPosition().getBinlogPosition()
                        + " } is lower or equal then last safe checkpoint position { "
                        + " binlog-filename => "
                        + pipelinePosition.getLastSafeCheckPointPosition().getBinlogFilename()
                        + ", binlog-position => "
                        + pipelinePosition.getLastSafeCheckPointPosition().getBinlogPosition()
                        + " }. Skipping event...");
                return true;
            }
        }

        switch (event.getHeader().getEventType()) {
            // Query Event:
            case MySQLConstants.QUERY_EVENT:

                switch (QueryInspector.getQueryEventType((QueryEvent) event)) {
                    case "BEGIN":
                    case "PSEUDOGTID":
                        return false;
                    case "COMMIT":
                        // COMMIT does not always contain database name so we get it
                        // from current transaction metadata.
                        // There is an assumption that all tables in the transaction
                        // are from the same database. Cross database transactions
                        // are not supported.
                        LOGGER.debug("Got commit event: " + event);
                        TableMapEvent firstMapEvent = currentTransactionMetadata.getFirstMapEventInTransaction();
                        if (firstMapEvent == null) {
                            LOGGER.warn(String.format(
                                    "Received COMMIT event, but currentTransactionMetadata is empty! Tables in transaction are %s",
                                    Joiner.on(", ").join(currentTransactionMetadata.getCurrentTransactionTableMapEvents().keySet())
                                    )
                            );
                            dropTransaction();
                            return true;
                            //throw new TransactionException("Got COMMIT while not in transaction: " + currentTransactionMetadata);
                        }

                        String currentTransactionDBName = firstMapEvent.getDatabaseName().toString();
                        if (!isReplicant(currentTransactionDBName)) {
                            LOGGER.warn(String.format("non-replicated database %s in current transaction.",
                                    currentTransactionDBName));
                            dropTransaction();
                            return true;
                        }

                        return false;
                    case "DDLTABLE":
                        // DDL event should always contain db name
                        String dbName = ((QueryEvent) event).getDatabaseName().toString();
                        if ((dbName == null) || dbName.length() == 0) {
                            LOGGER.warn("No Db name in Query Event. Extracted SQL: " + ((QueryEvent) event).getSql().toString());
                        }
                        if (isReplicant(dbName)) {
                            // process event
                            return false;
                        }
                        // skip event
                        LOGGER.warn("DDL statement " + ((QueryEvent) event).getSql() + " on non-replicated database: " + dbName + "");
                        return true;
                    case "DDLVIEW":
                        // TODO: handle View statement
                        return true;
                    case "ANALYZE":
                        return true;
                    default:
                        LOGGER.warn("Skipping event with unknown query type: " + ((QueryEvent) event).getSql());
                        return false;
                }

            // TableMap event:
            case MySQLConstants.TABLE_MAP_EVENT:
                return !isReplicant(((TableMapEvent) event).getDatabaseName().toString());
            // Data event:
            case MySQLConstants.UPDATE_ROWS_EVENT:
            case MySQLConstants.UPDATE_ROWS_EVENT_V2:
            case MySQLConstants.WRITE_ROWS_EVENT:
            case MySQLConstants.WRITE_ROWS_EVENT_V2:
            case MySQLConstants.DELETE_ROWS_EVENT:
            case MySQLConstants.DELETE_ROWS_EVENT_V2:
                return currentTransactionMetadata.getFirstMapEventInTransaction() == null;
            case MySQLConstants.XID_EVENT:
//                if (!currentTransactionMetadata.hasMappingInTransaction()) {
//                    if (currentTransactionMetadata.getEvents().size() == 0) {
//                        throw new TransactionException("Got COMMIT while not in transaction: " + currentTransactionMetadata);
//                    } else if (currentTransactionMetadata.getEvents().size() == 1) {
//                        BinlogEventV4 bufferedEvent = currentTransactionMetadata.getEvents().peek();
//                        if (bufferedEvent.getHeader().getEventType() == MySQLConstants.QUERY_EVENT && queryInspector.getQueryEventType((QueryEvent) bufferedEvent) == "BEGIN") {
//                            // empty transaction or all of the events in between have been dropped. Skipping
//                            dropTransaction();
//                            return true;
//                        }
//                    }
//                    LOGGER.warn(String.format(
//                            "Received COMMIT event, but currentTransactionMetadata is empty! Tables in transaction are %s",
//                            Joiner.on(", ").join(currentTransactionMetadata.getCurrentTransactionTableMapEvents().keySet())
//                            )
//                    );
//                    throw new TransactionException("Received COMMIT event, but currentTransactionMetadata is empty!" + currentTransactionMetadata);
//                    //dropTransaction();
//                    //return true;
//                    //throw new TransactionException("Got COMMIT while not in transaction: " + currentTransactionMetadata);
//                }
                return false;

            case MySQLConstants.ROTATE_EVENT:
                // This is a  workaround for a bug in open replicator
                // which results in rotate event being created twice per
                // binlog file - once at the end of the binlog file (as it should be)
                // and once at the beginning of the next binlog file (which is a bug)
                String currentBinlogFile =
                        pipelinePosition.getCurrentPosition().getBinlogFilename();
                if (rotateEventAllreadySeenForBinlogFile.containsKey(currentBinlogFile)) {
                    return true;
                }
                rotateEventAllreadySeenForBinlogFile.put(currentBinlogFile, true);
                return false;
            case MySQLConstants.FORMAT_DESCRIPTION_EVENT:
            case MySQLConstants.STOP_EVENT:
                return false;
            default:
                LOGGER.warn("Unexpected event type => " + event.getHeader().getEventType());
                return true;
        }
    }

    public boolean beginTransaction() {
        // a manual transaction beginning
        if (currentTransactionMetadata != null) {
            return false;
        }
        currentTransactionMetadata = new CurrentTransactionMetadata();
        LOGGER.debug("Started transaction " + currentTransactionMetadata.getUuid() + " without event");
        return true;
    }

    public boolean beginTransaction(QueryEvent event) {
        // begin a transaction with BEGIN query event
        if (currentTransactionMetadata != null) {
            return false;
        }
        currentTransactionMetadata = new CurrentTransactionMetadata(event);
        LOGGER.debug("Started transaction " + currentTransactionMetadata.getUuid() + " with event: " + event);
        return true;
    }

    public void addEventIntoTransaction(BinlogEventV4 event) throws TransactionException, TransactionSizeLimitException {
        if (!isInTransaction()) {
            throw new TransactionException("Failed to add new event into a transaction buffer while not in transaction: " + event);
        }
        if (isTransactionSizeLimitExceeded()) {
            throw new TransactionSizeLimitException();
        }
        currentTransactionMetadata.addEvent(event);
        if (currentTransactionMetadata.getEventsCounter() % 10 == 0) {
            LOGGER.debug("Number of events in current transaction " + currentTransactionMetadata.getUuid() + " is: " + currentTransactionMetadata.getEventsCounter());
        }
    }

    private void applyTransactionWithRewinding(QueryEvent oldBeginEvent) throws Exception {
        LOGGER.debug("Applying transaction with rewinding");
        if (isRewinding) {
            throw new RuntimeException("Recursive rewinding detected. CurrentTransactionMetadata:" + currentTransactionMetadata + ", oldBeginEvent: " + oldBeginEvent);
        }
        isRewinding = true;

        LOGGER.debug("Start rewinding transaction from: " + oldBeginEvent.getBinlogFilename() + ":" + oldBeginEvent.getHeader().getPosition());

        // drop events from current transaction
        dropTransaction();

        // get next xid event and skip everything before
        XidEvent xidEvent = (XidEvent) rewindToEventType(XidEvent.class.toString());
        beginTransaction(oldBeginEvent);

        // set binlog pos to begin pos, start openReplicator and apply the xid data to all events
        try {
            binlogEventProducer.stopAndClearQueue(10000, TimeUnit.MILLISECONDS);
            binlogEventProducer.setBinlogFileName(oldBeginEvent.getBinlogFilename());
            binlogEventProducer.setBinlogPosition(oldBeginEvent.getHeader().getNextPosition());
            binlogEventProducer.start();
        } catch (Exception e) {
            throw new BinlogEventProducerException("Can't stop binlogEventProducer to rewind a stream to the end of a transaction: ");
        }
        // TODO: no applying happens
        processQueueLoop(new BinlogPositionInfo(replicantPool.getReplicantDBActiveHostServerID(), xidEvent.getBinlogFilename(), xidEvent.getHeader().getPosition()));

        isRewinding = false;

        LOGGER.debug("Stop rewinding transaction at: " + xidEvent.getBinlogFilename() + ":" + xidEvent.getHeader().getPosition());
        // skip the xid event
    }

    public boolean isInTransaction() {
        return (currentTransactionMetadata != null);
    }

    public void commitTransaction(long timestamp, long xid) {
        // manual transaction commit
        currentTransactionMetadata.setXid(xid);
        currentTransactionMetadata.doTimestampOverride(timestamp);
        commitTransaction();
    }

    public void commitTransaction(XidEvent xidEvent) {
        currentTransactionMetadata.setFinishEvent(xidEvent);
        commitTransaction();
    }

    public void commitTransaction(QueryEvent queryEvent) {
        currentTransactionMetadata.setFinishEvent(queryEvent);
        commitTransaction();
    }

    private void commitTransaction() {
        // apply all the buffered events
        LOGGER.debug("Committing transaction uuid: " + currentTransactionMetadata.getUuid() + ", id: " + currentTransactionMetadata.getXid());
        // apply changes from buffer and pass current metadata with xid and uuid

        try {
            if (isRewinding) {
                applyTransactionFinishEvent();
            } else {
                if (isEmptyTransaction()) {
                    LOGGER.debug("Transaction is empty");
                    dropTransaction();
                    return;
                }
                applyTransactionBeginEvent();
                applyTransactionDataEvents();
                applyTransactionFinishEvent();
            }
        } catch (EventHandlerApplyException e) {
            LOGGER.error("Failed to commit transaction: " + currentTransactionMetadata, e);
            requestShutdown();
        }
        currentTransactionMetadata = null;
    }

    private void applyTransactionBeginEvent() throws EventHandlerApplyException {
        // apply begin event
        if (currentTransactionMetadata.hasBeginEvent()) {
            eventDispatcher.apply(currentTransactionMetadata.getBeginEvent(), currentTransactionMetadata);
        }
    }

    private void applyTransactionDataEvents() throws EventHandlerApplyException {
        // apply data-changing events
        for (BinlogEventV4 event : currentTransactionMetadata.getEvents()) {
            eventDispatcher.apply(event, currentTransactionMetadata);
        }
        currentTransactionMetadata.clearEvents();
    }

    private void applyTransactionFinishEvent() throws EventHandlerApplyException {
        // apply commit event
        if (currentTransactionMetadata.hasFinishEvent()) {
            eventDispatcher.apply(currentTransactionMetadata.getFinishEvent(), currentTransactionMetadata);
        }
    }

    private boolean isEmptyTransaction() {
        return (currentTransactionMetadata.hasFinishEvent() && !currentTransactionMetadata.hasEvents());
    }

    private void dropTransaction() {
        LOGGER.debug("Transaction dropped");
        currentTransactionMetadata = null;
    }

    public CurrentTransactionMetadata getCurrentTransactionMetadata() {
        return currentTransactionMetadata;
    }

    private boolean isTransactionSizeLimitExceeded() {
        return (currentTransactionMetadata.getEventsCounter() > TRANSACTION_SIZE_LIMIT);
    }

    // TODO: do we need it with transactions?
    private void doTimestampOverride(BinlogEventV4 event) {
        if (configuration.isInitialSnapshotMode()) {
            doInitialSnapshotEventTimestampOverride(event);
        } else {
            injectFakeMicroSecondsIntoEventTimestamp(event);
        }
    }

    private void injectFakeMicroSecondsIntoEventTimestamp(BinlogEventV4 event) {

        long overriddenTimestamp = event.getHeader().getTimestamp();

        if (overriddenTimestamp != 0) {
            // timestamp is in millisecond form, but the millisecond part is actually 000 (for example 1447755881000)
            overriddenTimestamp = (overriddenTimestamp * 1000) + fakeMicrosecondCounter;
            ((BinlogEventV4HeaderImpl)(event.getHeader())).setTimestamp(overriddenTimestamp);
        }
    }

    // set initial snapshot time to unix epoch.
    private void doInitialSnapshotEventTimestampOverride(BinlogEventV4 event) {

        long overriddenTimestamp = event.getHeader().getTimestamp();

        if (overriddenTimestamp != 0) {
            overriddenTimestamp = 0;
            ((BinlogEventV4HeaderImpl)(event.getHeader())).setTimestamp(overriddenTimestamp);
        }
    }
}
