/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.debezium.reader;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.debezium.task.MySqlBinlogSplitReadTask;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.flink.cdc.connectors.mysql.source.utils.ChunkUtils;
import org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupMode;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils.createBinaryClient;
import static org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils.createMySqlConnection;
import static org.apache.flink.cdc.connectors.mysql.source.utils.RecordUtils.isEndWatermarkEvent;

/**
 * Debezium binlog reader 实现。
 *
 * <p>负责读取 binlog，并过滤掉与 {@link SnapshotSplitReader} 已读取快照范围重叠的数据。
 */
public class BinlogSplitReader implements DebeziumReader<SourceRecords, MySqlSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogSplitReader.class);
    private final StatefulTaskContext statefulTaskContext;
    private final ExecutorService executorService;

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile boolean currentTaskRunning;
    private volatile Throwable readException;

    private MySqlBinlogSplitReadTask binlogSplitReadTask;
    private MySqlBinlogSplit currentBinlogSplit;
    private Map<TableId, List<FinishedSnapshotSplitInfo>> finishedSplitsInfo;
    // tableId -> 该表所有 snapshot split 中最大的 high watermark。
    private Map<TableId, BinlogOffset> maxSplitHighWatermarkMap;
    private final Set<TableId> pureBinlogPhaseTables;
    private Predicate capturedTableFilter;
    private final StoppableChangeEventSourceContext changeEventSourceContext =
            new StoppableChangeEventSourceContext();
    private final boolean isParsingOnLineSchemaChanges;
    private final boolean isBackfillSkipped;

    private static final long READER_CLOSE_TIMEOUT = 30L;

    public BinlogSplitReader(MySqlSourceConfig sourceConfig, int subtaskId) {
        this(
                new StatefulTaskContext(
                        sourceConfig,
                        createBinaryClient(sourceConfig.getDbzConfiguration()),
                        createMySqlConnection(sourceConfig)),
                subtaskId);
    }

    public BinlogSplitReader(StatefulTaskContext statefulTaskContext, int subtaskId) {
        this.statefulTaskContext = statefulTaskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("binlog-reader-" + subtaskId).build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.currentTaskRunning = true;
        this.pureBinlogPhaseTables = new HashSet<>();
        this.isParsingOnLineSchemaChanges =
                statefulTaskContext.getSourceConfig().isParseOnLineSchemaChanges();
        this.isBackfillSkipped = statefulTaskContext.getSourceConfig().isSkipSnapshotBackfill();
    }

    public void submitSplit(MySqlSplit mySqlSplit) {
        this.currentBinlogSplit = mySqlSplit.asBinlogSplit();
        configureFilter();
        statefulTaskContext.configure(currentBinlogSplit);
        this.capturedTableFilter = statefulTaskContext.getSourceConfig().getTableFilter();
        this.queue = statefulTaskContext.getQueue();
        this.binlogSplitReadTask =
                new MySqlBinlogSplitReadTask(
                        statefulTaskContext.getConnectorConfig(),
                        statefulTaskContext.getConnection(),
                        statefulTaskContext.getDispatcher(),
                        statefulTaskContext.getSignalEventDispatcher(),
                        statefulTaskContext.getErrorHandler(),
                        StatefulTaskContext.getClock(),
                        statefulTaskContext.getTaskContext(),
                        (MySqlStreamingChangeEventSourceMetrics)
                                statefulTaskContext.getStreamingChangeEventSourceMetrics(),
                        currentBinlogSplit,
                        createEventFilter());

        executorService.submit(
                () -> {
                    try {
                        binlogSplitReadTask.execute(
                                changeEventSourceContext,
                                statefulTaskContext.getMySqlPartition(),
                                statefulTaskContext.getOffsetContext());
                    } catch (Throwable t) {
                        LOG.error(
                                String.format(
                                        "Execute binlog read task for mysql split %s fail",
                                        currentBinlogSplit),
                                t);
                        readException = t;
                    }
                });
    }

    @Override
    public boolean isFinished() {
        return currentBinlogSplit == null || !currentTaskRunning;
    }

    @Nullable
    @Override
    public Iterator<SourceRecords> pollSplitRecords() throws InterruptedException {
        checkReadException();
        final List<SourceRecord> sourceRecords = new ArrayList<>();
        if (currentTaskRunning) {
            List<DataChangeEvent> batch = queue.poll();
            for (DataChangeEvent event : batch) {
                if (isEndWatermarkEvent(event.getRecord())) {
                    LOG.info("Read split {} end watermark event", currentBinlogSplit);
                    try {
                        stopBinlogReadTask();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    break;
                }

                if (isParsingOnLineSchemaChanges) {
                    Optional<SourceRecord> oscRecord =
                            parseOnLineSchemaChangeEvent(event.getRecord());
                    if (oscRecord.isPresent()) {
                        sourceRecords.add(oscRecord.get());
                        continue;
                    }
                }
                if (shouldEmit(event.getRecord())) {
                    sourceRecords.add(event.getRecord());
                }
            }
            List<SourceRecords> sourceRecordsSet = new ArrayList<>();
            sourceRecordsSet.add(new SourceRecords(sourceRecords));
            return sourceRecordsSet.iterator();
        } else {
            return null;
        }
    }

    private void checkReadException() {
        if (readException != null) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Read split %s error due to %s.",
                            currentBinlogSplit, readException.getMessage()),
                    readException);
        }
    }

    @Override
    public void close() {
        try {
            stopBinlogReadTask();
            if (statefulTaskContext != null) {
                statefulTaskContext.close();
            }
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(READER_CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Failed to close the binlog split reader in {} seconds.",
                            READER_CLOSE_TIMEOUT);
                }
            }
        } catch (Exception e) {
            LOG.error("Close binlog reader error", e);
        }
    }

    private Optional<SourceRecord> parseOnLineSchemaChangeEvent(SourceRecord sourceRecord) {
        if (RecordUtils.isOnLineSchemaChangeEvent(sourceRecord)) {
            // 这是 gh-ost 初始化的 schema change 事件，仅当去壳后的 tableId 命中过滤条件时才下发。
            TableId originalTableId = RecordUtils.getTableId(sourceRecord);
            TableId peeledTableId = RecordUtils.peelTableId(originalTableId);
            if (capturedTableFilter.test(peeledTableId)) {
                return Optional.of(
                        RecordUtils.setTableId(sourceRecord, originalTableId, peeledTableId));
            }
        }
        return Optional.empty();
    }

    /**
     * 判断该记录是否应该下发。
     *
     * <p>水位线过滤算法：binlog split reader 只发送“属于已完成 snapshot split 且位点晚于该 split 的
     * high watermark”的 binlog 事件。
     *
     * <pre> E.g: the data input is :
     *    snapshot-split-0 info : [0,    1024) highWatermark0
     *    snapshot-split-1 info : [1024, 2048) highWatermark1
     *  the data output is:
     *  only the binlog event belong to [0,    1024) and offset is after highWatermark0 should send,
     *  only the binlog event belong to [1024, 2048) and offset is after highWatermark1 should send.
     * </pre>
     */
    private boolean shouldEmit(SourceRecord sourceRecord) {
        if (RecordUtils.isDataChangeRecord(sourceRecord)) {
            TableId tableId = RecordUtils.getTableId(sourceRecord);
            if (pureBinlogPhaseTables.contains(tableId)) {
                return true;
            }
            BinlogOffset position = RecordUtils.getBinlogPosition(sourceRecord);
            if (hasEnterPureBinlogPhase(tableId, position)) {
                return true;
            }

            // 只有存在 snapshot split 历史的表需要做区间过滤。
            if (finishedSplitsInfo.containsKey(tableId)) {
                // 若配置为跳过 backfill，则无需过滤。
                if (isBackfillSkipped) {
                    return true;
                }
                RowType splitKeyType =
                        ChunkUtils.getChunkKeyColumnType(
                                statefulTaskContext.getDatabaseSchema().tableFor(tableId),
                                statefulTaskContext.getSourceConfig().getChunkKeyColumns(),
                                statefulTaskContext.getSourceConfig().isTreatTinyInt1AsBoolean());

                Struct target = RecordUtils.getStructContainsChunkKey(sourceRecord);
                Object[] chunkKey =
                        RecordUtils.getSplitKey(
                                splitKeyType, statefulTaskContext.getSchemaNameAdjuster(), target);
                for (FinishedSnapshotSplitInfo splitInfo : finishedSplitsInfo.get(tableId)) {
                    if (RecordUtils.splitKeyRangeContains(
                                    chunkKey, splitInfo.getSplitStart(), splitInfo.getSplitEnd())
                            && position.isAfter(splitInfo.getHighWatermark())) {
                        return true;
                    }
                }
            }
            // 不在已监控 split 范围内，不下发。
            return false;
        } else if (RecordUtils.isSchemaChangeEvent(sourceRecord)) {
            if (RecordUtils.isTableChangeRecord(sourceRecord)) {
                TableId tableId = RecordUtils.getTableId(sourceRecord);
                return capturedTableFilter.test(tableId);
            } else {
                // 与表结构无关（如 `CREATE/DROP DATABASE`）的事件跳过。
                return false;
            }
        }
        return true;
    }

    private boolean hasEnterPureBinlogPhase(TableId tableId, BinlogOffset position) {
        // 已完成 snapshot 的存量表：位点超过最大 high watermark 后进入纯 binlog 阶段。
        if (maxSplitHighWatermarkMap.containsKey(tableId)
                && position.isAfter(maxSplitHighWatermarkMap.get(tableId))) {
            pureBinlogPhaseTables.add(tableId);
            return true;
        }

        // 若关闭“扫描新增表”，仍需捕获新增分片表（无历史 split 记录）的 binlog 事件。
        // 若开启“扫描新增表”，从 savepoint/checkpoint 恢复后会补齐新增表（含分片表与普通表）的历史记录。
        if (!statefulTaskContext.getSourceConfig().isScanNewlyAddedTableEnabled()) {
            // 无历史 high watermark 且命中过滤条件的新增分片表。
            return !maxSplitHighWatermarkMap.containsKey(tableId)
                    && capturedTableFilter.test(tableId);
        }
        return false;
    }

    private void configureFilter() {
        List<FinishedSnapshotSplitInfo> finishedSplitInfos =
                currentBinlogSplit.getFinishedSnapshotSplitInfos();
        Map<TableId, List<FinishedSnapshotSplitInfo>> splitsInfoMap = new HashMap<>();
        Map<TableId, BinlogOffset> tableIdBinlogPositionMap = new HashMap<>();
        // 纯流模式启动：没有 snapshot split，直接以起始位点作为表级基准位点。
        if (finishedSplitInfos.isEmpty()) {
            for (TableId tableId : currentBinlogSplit.getTableSchemas().keySet()) {
                tableIdBinlogPositionMap.put(tableId, currentBinlogSplit.getStartingOffset());
            }
        }
        // initial 模式：按表聚合 finished snapshot split，并记录每张表最大的 high watermark。
        else {
            for (FinishedSnapshotSplitInfo finishedSplitInfo : finishedSplitInfos) {
                TableId tableId = finishedSplitInfo.getTableId();
                List<FinishedSnapshotSplitInfo> list =
                        splitsInfoMap.getOrDefault(tableId, new ArrayList<>());
                list.add(finishedSplitInfo);
                splitsInfoMap.put(tableId, list);

                BinlogOffset highWatermark = finishedSplitInfo.getHighWatermark();
                BinlogOffset maxHighWatermark = tableIdBinlogPositionMap.get(tableId);
                if (maxHighWatermark == null || highWatermark.isAfter(maxHighWatermark)) {
                    tableIdBinlogPositionMap.put(tableId, highWatermark);
                }
            }
        }
        this.finishedSplitsInfo = splitsInfoMap;
        this.maxSplitHighWatermarkMap = tableIdBinlogPositionMap;
        this.pureBinlogPhaseTables.clear();
    }

    private Predicate<Event> createEventFilter() {
        // TIMESTAMP 启动模式下需要过滤事件，丢弃早于指定时间戳的行变更事件。

        // 注意：这里以用户当前配置（statefulTaskContext.getSourceConfig()）为准。
        // 若用户改配置后再从 savepoint/checkpoint 恢复，可能与保存状态产生冲突；
        // 但当前并不承诺“改配置后 checkpoint 兼容”，因此该行为可接受。
        StartupOptions startupOptions = statefulTaskContext.getSourceConfig().getStartupOptions();
        if (startupOptions.startupMode.equals(StartupMode.TIMESTAMP)) {
            if (startupOptions.binlogOffset == null) {
                throw new NullPointerException(
                        "The startup option was set to TIMESTAMP "
                                + "but unable to find starting binlog offset. Please check if the timestamp is specified in "
                                + "configuration. ");
            }
            long startTimestampSec = startupOptions.binlogOffset.getTimestampSec();
            // 仅过滤数据变更事件；其他事件仍需保留，以维护 MySqlStreamingChangeEventSource 内部状态。
            LOG.info(
                    "Creating event filter that dropping row mutation events before timestamp in second {}",
                    startTimestampSec);
            return event -> {
                if (!EventType.isRowMutation(getEventType(event))) {
                    return true;
                }
                return event.getHeader().getTimestamp() >= startTimestampSec * 1000;
            };
        }
        return event -> true;
    }

    public void stopBinlogReadTask() {
        currentTaskRunning = false;
        // 终止 MySqlStreamingChangeEventSource.execute() 内部 while 循环。
        changeEventSourceContext.stopChangeEventSource();
    }

    private EventType getEventType(Event event) {
        return event.getHeader().getEventType();
    }

    @VisibleForTesting
    public ExecutorService getExecutorService() {
        return executorService;
    }

    @VisibleForTesting
    MySqlBinlogSplitReadTask getBinlogSplitReadTask() {
        return binlogSplitReadTask;
    }

    @VisibleForTesting
    public StoppableChangeEventSourceContext getChangeEventSourceContext() {
        return changeEventSourceContext;
    }
}
