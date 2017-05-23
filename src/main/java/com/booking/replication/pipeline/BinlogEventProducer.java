package com.booking.replication.pipeline;

import static com.codahale.metrics.MetricRegistry.name;

import com.booking.replication.Configuration;
import com.booking.replication.Constants;
import com.booking.replication.Metrics;

import com.booking.replication.binlog.BinlogEventParserProviderCode;
import com.booking.replication.binlog.BinlogEventParserProviderFactory;
import com.booking.replication.binlog.RawBinlogEvent;
import com.booking.replication.replicant.ReplicantPool;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.event.Event;

import com.google.code.or.OpenReplicator;
import com.google.code.or.binlog.BinlogEventListener;
import com.google.code.or.binlog.BinlogEventV4;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;

/**
 * Simple wrapper for Open Replicator. Writes events to blocking queue.
 */
public class BinlogEventProducer {

    // queue is private, but it will reference the same queue
    // as the consumer object

    private final BlockingQueue<RawBinlogEvent> rawBinlogEventQueue;

    private final PipelinePosition pipelinePosition;

    private final Object           binlogEventParserProvider;

    private final int              BINLOG_EVENT_PARSER_PROVIDER_CODE;

    private final Configuration    configuration;
    private final ReplicantPool    replicantPool;

    private long opCounter = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogEventProducer.class);

    private static final Meter producedEvents = Metrics.registry.meter(name("events", "eventsProduced"));

    /**
     * Set up and manage the binlog provider instance instance.
     * @param rawBinlogEventQueue
     * @param pipelinePosition  Binlog position information
     * @param configuration     Replicator configuration
     * @param binlogParserProviderCode
     */
    public BinlogEventProducer(

        LinkedBlockingQueue<RawBinlogEvent> rawBinlogEventQueue,
        PipelinePosition                    pipelinePosition,
        Configuration                       configuration,
        ReplicantPool                       replicantPool,
        int                                 binlogParserProviderCode

    ) throws Exception {

        this.rawBinlogEventQueue = rawBinlogEventQueue;
        this.pipelinePosition    = pipelinePosition;
        this.configuration       = configuration;
        this.replicantPool       = replicantPool;

        this.BINLOG_EVENT_PARSER_PROVIDER_CODE = binlogParserProviderCode;

        Random random = new Random();

        int serverId = (random.nextInt() >>> 1) | (1 << 30); // a large positive random integer

        binlogEventParserProvider =
            BinlogEventParserProviderFactory.getBinlogEventParserProvider(
                    serverId,
                    BINLOG_EVENT_PARSER_PROVIDER_CODE,
                    configuration,
                    pipelinePosition
            );

        Metrics.registry.register(name("events", "producerBackPressureSleep"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return backPressureSleep;
                    }
                });

    }

    private void startBinaryLogClient(BinaryLogClient binaryLogClient) throws IOException, Exception {

        binaryLogClient.registerEventListener(new EventListener() {

            @Override
            public void onEvent(Event event) {
                producedEvents.mark();

                // This call is blocking the writes from server side. If time goes above
                // net_write_timeout (which defaults to 60s) server will drop connection.
                //      => Use back pressure to regulate the write rate to the queue.

                if (isRunning()) {
                    boolean eventQueued = false;
                    while (!eventQueued) { // blocking block
                        try {
                            backPressureSleep();

                            // TODO: get the proper type queue from RawBInlogEventsQueue (
                            // or add push method that can accept multiple event types)
                            boolean added = false;
                            try {
                                RawBinlogEvent rawBinlogEvent = new RawBinlogEvent(event);

                                // binlog file name of the last red event. Inject in the
                                // raw event object since we need it later for each event.
                                rawBinlogEvent.setBinlogFilename(binaryLogClient.getBinlogFilename());
                                // TODO: set in the RawBinlogEvent constructor and make immutable

                                added = rawBinlogEventQueue.offer(rawBinlogEvent, 100, TimeUnit.MILLISECONDS);
                            } catch (Exception e) {
                                LOGGER.error("rawBinlogEventsQueue.offer failed.", e);
                            }

                            if (added) {
                                opCounter++;
                                eventQueued = true;
                                if (opCounter % 100000 == 0) {
                                    LOGGER.info("Producer reporting queue size => " + rawBinlogEventQueue.size());
                                }
                            } else {
                                LOGGER.error("queue.offer timed out. Will sleep for 100ms and try again");
                                Thread.sleep(100);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        LOGGER.info("starting BinaryLogClient from: { binlog-file => "
                + binaryLogClient.getBinlogFilename()
                + ", position => "
                + binaryLogClient.getBinlogPosition()
                + " }"
        );
        binaryLogClient.connect();
    }

    /**
     * Start.
     */
    public void startOpenReplicator(OpenReplicator openReplicator) throws Exception {

        openReplicator.setBinlogEventListener(new BinlogEventListener() {

            public void onEvents(BinlogEventV4 event) {
                producedEvents.mark();

                // This call is blocking the writes from server side. If time goes above
                // net_write_timeout (which defaults to 60s) server will drop connection.
                //      => Use back pressure to regulate the write rate to the queue.

                if (isRunning()) {
                    boolean eventQueued = false;
                    while (!eventQueued) { // blocking block
                        try {
                            backPressureSleep();
                            boolean added = false;
                            try {
                                RawBinlogEvent rawBinlogEvent = new RawBinlogEvent(event);
                                added = rawBinlogEventQueue.offer(rawBinlogEvent, 100, TimeUnit.MILLISECONDS);
                            } catch (Exception e) {
                                LOGGER.error("rawBinlogEventsQueue.offer failed ", e);
                            }

                            if (added) {
                                opCounter++;
                                eventQueued = true;
                                if (opCounter % 100000 == 0) {
                                    LOGGER.info("Producer reporting queue size => " + rawBinlogEventQueue.size());
                                }
                            } else {
                                LOGGER.error("queue.offer timed out. Will sleep for 100ms and try again");
                                Thread.sleep(100);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        LOGGER.info("starting Open Replicator from: { binlog-file => "
                + openReplicator.getBinlogFileName()
                + ", position => "
                + openReplicator.getBinlogPosition()
                + " }"
        );
        openReplicator.start();
    }

    private long backPressureSleep = 0;

    private void backPressureSleep() {
        int queueSize = rawBinlogEventQueue.size();

        // For an explanation please plug "max(0, 20*(10000/(10000+1-x)-10)) x from 6000 to 10000" into WolframAlpha
        backPressureSleep = Math.max(
            20 * ( Constants.MAX_RAW_QUEUE_SIZE / (Constants.MAX_RAW_QUEUE_SIZE + 1 - queueSize) - 10),
            0
        );

        // Queue size is 9243.(42 period) (where MAX_RAW_QUEUE_SIZE = 10000)
        if (backPressureSleep < 64) {
            return;
        }

        // Queue size is 9953.(380952 period)
        if (backPressureSleep > 4000) {
            LOGGER.warn("Queue is getting big, back pressure is getting high");
        }

        try {
            Thread.sleep(backPressureSleep);
        } catch (InterruptedException e) {
            LOGGER.error("Thread wont sleep");
            e.printStackTrace();
        }
    }

    public void stopOpenReplicator(OpenReplicator openReplicator, long timeout, TimeUnit unit) throws Exception {
        openReplicator.stop(timeout, unit);
    }

    public void stopBinaryLogClient (BinaryLogClient binaryLogClient, long timeout, TimeUnit unit) throws Exception {
        binaryLogClient.disconnect();
    }

    public boolean isRunning() {
        if (BINLOG_EVENT_PARSER_PROVIDER_CODE == BinlogEventParserProviderCode.OR) {
            return ((OpenReplicator)binlogEventParserProvider).isRunning();
        }
        else { // if (BINLOG_EVENT_PARSER_PROVIDER_CODE == BinlogEventParserProviderCode.OR) {
            return  ((BinaryLogClient)binlogEventParserProvider).isConnected();
        }
    }

    public void stop(long timeout, TimeUnit unit) throws Exception {
        if (BINLOG_EVENT_PARSER_PROVIDER_CODE == BinlogEventParserProviderCode.OR) {
            stopOpenReplicator((OpenReplicator)binlogEventParserProvider,  timeout, unit);
        }
        else { // if (BINLOG_EVENT_PARSER_PROVIDER_CODE == BinlogEventParserProviderCode.OR) {
            stopBinaryLogClient((BinaryLogClient)binlogEventParserProvider,timeout, unit);
        }
    }

    public void start() throws Exception {
        if (binlogEventParserProvider instanceof OpenReplicator) {
            startOpenReplicator((OpenReplicator) binlogEventParserProvider);
        }
        else if (binlogEventParserProvider instanceof BinaryLogClient) {
            startBinaryLogClient((BinaryLogClient) binlogEventParserProvider);
        }
        else {
            throw new Exception("Unsupported parser exception");
        }
    }
}
