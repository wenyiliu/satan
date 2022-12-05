package org.satan.flink11.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.satan.flink11.entity.HBaseRowData;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author liuwenyi
 * @date 2022/10/15
 **/
@Slf4j
public class HbaseSink extends RichSinkFunction<HBaseRowData> implements CheckpointedFunction, BufferedMutator.ExceptionListener {

    private String key;

    private transient Connection conn = null;
    private transient BufferedMutator mutator;
    private final transient ScheduledExecutorService executor;
    private transient ScheduledFuture scheduledFuture;
    private transient AtomicLong numPendingRequests;
    private transient volatile boolean closed = false;
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    public HbaseSink(String key) {
        this.key = key;
        this.executor = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern(key).daemon(true).build());
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        String zkQuorum = "10.122.1.112";
        String clientPort = "2181";
        String tableName = "employees";

        // 根据不同环境的配置设置一下三个参数即可
        long bufferFlushMaxSizeInBytes = 10000L;
        long bufferFlushIntervalMillis = 1L;

        //链接服务器
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        conf.set("hbase.zookeeper.property.clientPort", clientPort);

        try {
            this.numPendingRequests = new AtomicLong(0);

            if (null == conn) {
                this.conn = ConnectionFactory.createConnection(conf);
            }
            BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName))
                    .listener(this)
                    .writeBufferSize(bufferFlushMaxSizeInBytes);
            mutator = conn.getBufferedMutator(params);
            this.scheduledFuture = this.executor.scheduleWithFixedDelay(() -> {
                if (closed) {
                    return;
                }
                try {
                    flush();
                } catch (Exception e) {
                    failureThrowable.compareAndSet(null, e);
                }
            }, bufferFlushIntervalMillis, bufferFlushIntervalMillis, TimeUnit.MILLISECONDS);
        } catch (TableNotFoundException tnfe) {
            log.error("The table " + tableName + " not found ", tnfe);
            throw new RuntimeException("HBase table '" + tableName + "' not found.", tnfe);
        } catch (IOException ioe) {
            log.error("Exception while creating connection to HBase.", ioe);
            throw new RuntimeException("Cannot create connection to HBase.", ioe);
        }

    }

    @Override
    public void invoke(HBaseRowData value, Context context) throws Exception {
        checkErrorAndRethrow();
    }

    private void flush() throws Exception {
        mutator.flush();
        numPendingRequests.set(0);
        checkErrorAndRethrow();
    }

    private void checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            throw new RuntimeException("An error occurred in HBaseSink.", cause);
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;

        if (mutator != null) {
            try {
                mutator.close();
            } catch (IOException e) {
                log.warn("Exception occurs while closing HBase BufferedMutator.", e);
            }
            this.mutator = null;
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                log.warn("Exception occurs while closing HBase Connection.", e);
            }
            this.conn = null;
        }

        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            if (executor != null) {
                executor.shutdownNow();
            }
        }

    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        while (numPendingRequests.get() != 0) {
            flush();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }

    @Override
    public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator) throws RetriesExhaustedWithDetailsException {
        failureThrowable.compareAndSet(null, e);
    }
}
