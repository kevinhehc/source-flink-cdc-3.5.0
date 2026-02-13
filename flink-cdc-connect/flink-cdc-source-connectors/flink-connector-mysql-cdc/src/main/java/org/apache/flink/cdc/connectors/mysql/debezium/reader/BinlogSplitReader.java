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
// 数据变更 不在当前 chunk 范围内的三种情况

// A. 新增行落在某个未来 chunk 的 key-range 内（该 chunk 还没 snapshot）
//例如：chunk 划分有 [0,10000), [10000,20000)，你正在读第一个 chunk 的时候插入了 id=15000。
//为什么不会丢？
//	•	在 binlog 阶段的过滤逻辑里：如果该表还没进入 pure binlog phase，并且事件不属于“已完成 snapshot split 且 offset>其HW”，
//	就 return false 不下发。源码里最后明确写着：// not in the monitored splits scope, do not emit return false。 ￼
//	•	也就是说：对还没完成 snapshot 的 chunk，其范围内的 binlog 事件不会提前发出来。
//	•	等到 [10000,20000) 这个 chunk 开始做 snapshot 时，id=15000 已经在表里了，会被这个 chunk 的 snapshot SQL 直接读出来。
//所以你不会丢数据，反而是有意避免“binlog 先发，snapshot 后发”造成同主键乱序
// （Hybrid Assigner 里也明确说了要等 snapshot assigner finished，否则会 out-of-order）。


// ￼B. 新增行落在已经完成 snapshot 的 chunk key-range 内（该 chunk 已经 snapshot 完）
//例如：你已经完成了 [0,10000) 的 chunk snapshot，之后插入了 id=5000。
//为什么不会丢？
//binlog reader 的 shouldEmit 对这类事件要求同时满足两条件（源码里写得一清二楚）：
//	1.	splitKeyRangeContains(chunkKey, splitStart, splitEnd) —— 事件的主键落在该 split 的 key-range 内
//	2.	position.isAfter(splitInfo.getHighWatermark()) —— 事件 offset 必须在该 split 的 HW 之后
//两者同时满足才 return true 放行。 ￼
//这正好保证了：
//	•	snapshot 期间（LOW~HIGH 区间内）属于该 chunk 的变更，会在“对齐/回填”窗口里被正确处理（避免重复/冲突）
//	•	snapshot 完成之后的新变更，会以 binlog 形式正常输出


// C. 新增行的主键超出了当初切 chunk 时看到的最大值（不在任何 chunk 分配范围内）
//这是你问题里最尖锐的那种：“我切分时 maxId=1,000,000，snapshot 过程中插入了 id=2,000,000，当初根本没分到任何 chunk 里，那怎么办？”
//这类数据也不会丢，靠的是 pure binlog phase：
//	•	BinlogSplitReader#hasEnterPureBinlogPhase(tableId, position)：当 binlog position 超过该表的 maxSplitHighWatermark
//  	（也就是全表 snapshot splits 的最大 HW）时，标记该表进入 pureBinlogPhaseTables，之后对该表事件直接 return true。 ￼
//	•	进入 pure binlog phase 之前：如果事件不属于已完成 splits 的 key-range，shouldEmit 会挡掉（return false）。 ￼
//	•	进入 pure binlog phase 之后：该表所有 row mutation 事件都放行（不再需要 key-range 过滤），
//   	因此像 id=2,000,000 这种“当初没覆盖到的 key-range”插入会在此时被捕获并输出。 ￼
//这里的关键是：maxSplitHighWatermarkMap 是在 configureFilter() 里由 FinishedSnapshotSplitInfo 聚合出来的，
// 也就是 Hybrid 创建 binlog split 时塞进去的每个 split 的 HW 列表。


public class BinlogSplitReader implements DebeziumReader<SourceRecords, MySqlSplit> {
// 用时间线把三类情况串起来（最直观）
//
//假设 chunk 划分按 id：
//	•	split0: [0,10000)，HW0
//	•	split1: [10000,20000)，HW1
//	•	……
//	•	splitN: [990000,1000000]，HWN
//	•	maxHW = max(HW0..HWN)
//
//binlog reader 的输出策略是：
//	1.	position ≤ maxHW：过滤阶段
//	•	只输出：落在某个“已完成 split 的范围”内 且 offset > 该 split.HW 的事件  ￼
//	•	不输出：未来 split 范围内的事件（避免提前发导致乱序/重复） ￼
//	•	不输出：超出所有 split 范围的事件（因为没法归属任何 split 做对齐） ￼
//	2.	position > maxHW：pure binlog phase
//	•	对该表事件全部输出（不再需要按 split 范围过滤） ￼
//
//因此：
//	•	未来 chunk 范围内的新增行：会被后续 snapshot chunk 读到（binlog 先不发）
//	•	已完成 chunk 范围内的新增/更新：在 offset > 该 chunk.HW 后由 binlog 发出
//	•	超出初始范围的新增行：在进入 pure binlog phase 后由 binlog 发出
//
//三条合起来就实现了“不丢”。


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
        // 如果是数据变更
        if (RecordUtils.isDataChangeRecord(sourceRecord)) {
            TableId tableId = RecordUtils.getTableId(sourceRecord);
            if (pureBinlogPhaseTables.contains(tableId)) {
                return true;
            }
            BinlogOffset position = RecordUtils.getBinlogPosition(sourceRecord);
            // 如果是纯binlog的阶段(snapshot的同步已经处理完了)
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
