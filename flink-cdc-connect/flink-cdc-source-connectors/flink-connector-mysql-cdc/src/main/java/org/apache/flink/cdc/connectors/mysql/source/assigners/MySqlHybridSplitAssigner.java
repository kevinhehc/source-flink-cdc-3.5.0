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

package org.apache.flink.cdc.connectors.mysql.source.assigners;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.HybridPendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;

import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * MySQL 混合分片分配器。
 *
 * <p>先按主键范围把表切成多个 snapshot split 分配出去；当所有 snapshot split 都完成后，再分配一个
 * binlog split 持续消费增量日志。
 */
// 先做 snapshot 分片，再“继续”进入 binlog split（全量 + 增量一体）。
public class MySqlHybridSplitAssigner implements MySqlSplitAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlHybridSplitAssigner.class);
    private static final String BINLOG_SPLIT_ID = "binlog-split";

    /** binlog split 中可直接携带的 finished-split 元信息分组阈值。 */
    private final int splitMetaGroupSize;
    private final MySqlSourceConfig sourceConfig;

    /** 当前是否已经分配过 binlog split。 */
    private boolean isBinlogSplitAssigned;

    /** 实际负责 snapshot split 生命周期管理的分配器。 */
    private final MySqlSnapshotSplitAssigner snapshotSplitAssigner;

    public MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext) {
        this(
                sourceConfig,
                new MySqlSnapshotSplitAssigner(
                        sourceConfig,
                        currentParallelism,
                        remainingTables,
                        isTableIdCaseSensitive,
                        enumeratorContext),
                false,
                sourceConfig.getSplitMetaGroupSize());
    }

    public MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            int currentParallelism,
            HybridPendingSplitsState checkpoint,
            SplitEnumeratorContext<MySqlSplit> enumeratorContext) {
        this(
                sourceConfig,
                new MySqlSnapshotSplitAssigner(
                        sourceConfig,
                        currentParallelism,
                        checkpoint.getSnapshotPendingSplits(),
                        enumeratorContext),
                checkpoint.isBinlogSplitAssigned(),
                sourceConfig.getSplitMetaGroupSize());
    }

    private MySqlHybridSplitAssigner(
            MySqlSourceConfig sourceConfig,
            MySqlSnapshotSplitAssigner snapshotSplitAssigner,
            boolean isBinlogSplitAssigned,
            int splitMetaGroupSize) {
        this.sourceConfig = sourceConfig;
        this.snapshotSplitAssigner = snapshotSplitAssigner;
        this.isBinlogSplitAssigned = isBinlogSplitAssigned;
        this.splitMetaGroupSize = splitMetaGroupSize;
    }

    @Override
    public void open() {
        snapshotSplitAssigner.open();
    }

    @Override
    public Optional<MySqlSplit> getNext() {
        // 新增表正在做 snapshot 分配时，先不下发 split，避免流程交叉。
        if (AssignerStatus.isNewlyAddedAssigningSnapshotFinished(getAssignerStatus())) {
            return Optional.empty();
        }
        if (snapshotSplitAssigner.noMoreSplits()) {
            // snapshot split 已经分配完，进入 binlog split 分配阶段。
            if (isBinlogSplitAssigned) {
                // binlog split 已分配过，不再有新 split。
                return Optional.empty();
            } else if (AssignerStatus.isInitialAssigningFinished(
                    snapshotSplitAssigner.getAssignerStatus())) {
                // 必须等初始 snapshot 全部结束后再分配 binlog split，
                // 否则同一主键的数据可能出现 snapshot 与 binlog 的乱序。
                isBinlogSplitAssigned = true;
                return Optional.of(createBinlogSplit());
            } else if (AssignerStatus.isNewlyAddedAssigningFinished(
                    snapshotSplitAssigner.getAssignerStatus())) {
                // 新增表流程完成时不创建新的 binlog split，只通过状态变化唤醒 binlog reader。
                isBinlogSplitAssigned = true;
                return Optional.empty();
            } else {
                // 还没到可以分配 binlog split 的时机。
                return Optional.empty();
            }
        } else {
            // snapshot split 还有剩余，继续从 snapshot 分配器取下一个 split。
            return snapshotSplitAssigner.getNext();
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return snapshotSplitAssigner.waitingForFinishedSplits();
    }

    @Override
    public List<FinishedSnapshotSplitInfo> getFinishedSplitInfos() {
        return snapshotSplitAssigner.getFinishedSplitInfos();
    }

    @Override
    public void onFinishedSplits(Map<String, BinlogOffset> splitFinishedOffsets) {
        snapshotSplitAssigner.onFinishedSplits(splitFinishedOffsets);
    }

    @Override
    public void addSplits(Collection<MySqlSplit> splits) {
        List<MySqlSplit> snapshotSplits = new ArrayList<>();
        for (MySqlSplit split : splits) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split);
            } else {
                // 不缓存 binlog split，后续会根据最新 snapshot 完成信息重新构建。
                isBinlogSplitAssigned = false;
            }
        }
        snapshotSplitAssigner.addSplits(snapshotSplits);
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new HybridPendingSplitsState(
                snapshotSplitAssigner.snapshotState(checkpointId), isBinlogSplitAssigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        snapshotSplitAssigner.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return snapshotSplitAssigner.getAssignerStatus();
    }

    @Override
    public boolean noMoreSplits() {
        return snapshotSplitAssigner.noMoreSplits() && isBinlogSplitAssigned;
    }

    @Override
    public void startAssignNewlyAddedTables() {
        snapshotSplitAssigner.startAssignNewlyAddedTables();
    }

    @Override
    public void onBinlogSplitUpdated() {
        snapshotSplitAssigner.onBinlogSplitUpdated();
    }

    @Override
    public void close() {
        snapshotSplitAssigner.close();
    }

    // --------------------------------------------------------------------------------------------

    /**
     * 基于所有已分配且已完成的 snapshot split 构建一个 binlog split。
     *
     * <p>会计算：
     *
     * <ul>
     *   <li>起始位点：所有 finished offset 的最小值；</li>
     *   <li>停止位点：snapshot-only 模式下取最大 finished offset，否则不停止；</li>
     *   <li>finished snapshot 元信息：供 reader 做去重与顺序衔接。</li>
     * </ul>
     */
    private MySqlBinlogSplit createBinlogSplit() {
        final List<MySqlSchemalessSnapshotSplit> assignedSnapshotSplit =
                snapshotSplitAssigner.getAssignedSplits().values().stream()
                        .sorted(Comparator.comparing(MySqlSplit::splitId))
                        .collect(Collectors.toList());

        Map<String, BinlogOffset> splitFinishedOffsets =
                snapshotSplitAssigner.getSplitFinishedOffsets();
        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();

        BinlogOffset minBinlogOffset = null;
        BinlogOffset maxBinlogOffset = null;
        for (MySqlSchemalessSnapshotSplit split : assignedSnapshotSplit) {
            // 计算所有 snapshot split 完成位点中的最小值和最大值。
            BinlogOffset binlogOffset = splitFinishedOffsets.get(split.splitId());
            if (minBinlogOffset == null || binlogOffset.isBefore(minBinlogOffset)) {
                minBinlogOffset = binlogOffset;
            }
            if (maxBinlogOffset == null || binlogOffset.isAfter(maxBinlogOffset)) {
                maxBinlogOffset = binlogOffset;
            }

            finishedSnapshotSplitInfos.add(
                    new FinishedSnapshotSplitInfo(
                            split.getTableId(),
                            split.splitId(),
                            split.getSplitStart(),
                            split.getSplitEnd(),
                            binlogOffset));
        }

        // snapshot-only 模式下，结束位点取最大 watermark，保证在同一快照时刻收敛。
        BinlogOffset stoppingOffset = BinlogOffset.ofNonStopping();
        if (sourceConfig.getStartupOptions().isSnapshotOnly()) {
            stoppingOffset = maxBinlogOffset;
        }

        // 元信息过大时不直接内嵌，改为后续分组下发，降低单次传输负载。
        boolean divideMetaToGroups = finishedSnapshotSplitInfos.size() > splitMetaGroupSize;
        return new MySqlBinlogSplit(
                BINLOG_SPLIT_ID,
                minBinlogOffset == null ? BinlogOffset.ofEarliest() : minBinlogOffset,
                stoppingOffset,
                divideMetaToGroups ? new ArrayList<>() : finishedSnapshotSplitInfos,
                new HashMap<>(),
                finishedSnapshotSplitInfos.size());
    }
}
