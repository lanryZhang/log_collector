package com.ifeng.logcollector.sinks;

import com.ifeng.logcollector.sources.EventBuilder;
import com.ifeng.logcollector.sources.ExecSource;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhanglr on 2016/9/28.
 */
public class AvroProxy implements IProxy{
    private NettyTransceiver client = null;
    private Logger logger = Logger.getLogger(AvroProxy.class);
    private AvroSourceProtocol proxy = null;
    private int batchSize;
    private String topic;
    private long batchTimeout;
    private List<AvroFlumeEvent> events = new ArrayList<>();
    private long lastFlushTime = System.currentTimeMillis();
    ScheduledExecutorService timedFlushService;
    ScheduledFuture<?> future;
    private boolean sync = false;

    public AvroProxy(String hostIp, int port, int bs, String topic, long bt,boolean sync) throws IOException {
        this.batchSize = bs;
        this.topic = topic;
        this.batchTimeout = bt;
        this.sync = sync;
        try {
            client = new NettyTransceiver(new InetSocketAddress(hostIp, port));

            SpecificRequestor.getClient(AvroSourceProtocol.class, client);
            proxy = SpecificRequestor.getClient(AvroSourceProtocol.class, client);
            timedFlushService = Executors.newSingleThreadScheduledExecutor();
            if (!sync) {
                future = timedFlushService.scheduleWithFixedDelay(
                        new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    synchronized (events) {
                                        if (!events.isEmpty() && timeout()) {
                                            logger.info("flush logs " + events.size());
                                            Status s = Status.FAILED;
                                            try {
                                                s = proxy.appendBatch(events);
                                            } catch (AvroRemoteException e) {
                                                logger.error(e);
                                            }

                                            if (s == Status.FAILED) {
                                                proxy.appendBatch(events);
                                                logger.error("timer executor fail to push log, events size:" + events.size()+"----"+events.get(0));

                                            }
                                            events.clear();
                                            lastFlushTime = System.currentTimeMillis();
                                        }
                                    }
                                } catch (Exception e) {
                                    logger.error("Exception occured when processing event batch", e);
                                    if (e instanceof InterruptedException) {
                                        Thread.currentThread().interrupt();
                                    }
                                }
                            }
                        },
                        batchTimeout, batchTimeout, TimeUnit.MILLISECONDS);
            }

        } catch (IOException e) {
            logger.error(e);
            throw e;
        }
    }

    @Override
    public void append(String line) {
        HashMap<String, String> header = new HashMap<>();
        header.put("log_from", topic);
        synchronized (events) {
            header = new HashMap<>();
            header.put("log_from", topic);
            events.add(EventBuilder.wrap(header, ByteBuffer.wrap(line.getBytes())));
            if (sync) {
                if (events.size() > batchSize || timeout()) {
                    //logger.info("flush logs " + events.size());
                    try {
                        Status s = proxy.appendBatch(events);
                        if (s == Status.FAILED) {
                            proxy.appendBatch(events);
                            logger.error("after append fail to push log, events size:" + events.size()+"----"+events.get(0));
                        }
                    } catch (AvroRemoteException e) {
                        logger.error(e);
                    }finally {
                        events.clear();
                    }

                    lastFlushTime = System.currentTimeMillis();
                }
            }
        }
    }
    @Override
    public void flush(){
        synchronized (events) {
            if(!events.isEmpty()) {
                try {
                    proxy.appendBatch(events);

                } catch (AvroRemoteException e) {
                    logger.error(e);
                }finally{
                    events.clear();
                }
            }
        }
    }

    @Override
    public void start() {

    }

    @Override
    public int getAliveWorkCount() {
        return 1;
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
        if (future != null){
            future.cancel(true);
        }
        if (timedFlushService != null){
            timedFlushService.shutdown();
            while (!timedFlushService.isTerminated()) {
                try {
                    timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.debug("Interrupted while waiting for exec executor service "
                            + "to stop. Just exiting.");
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    boolean timeout() {
        if ((System.currentTimeMillis() - lastFlushTime) > batchTimeout) {
            return true;
        }
        return false;
    }
}
