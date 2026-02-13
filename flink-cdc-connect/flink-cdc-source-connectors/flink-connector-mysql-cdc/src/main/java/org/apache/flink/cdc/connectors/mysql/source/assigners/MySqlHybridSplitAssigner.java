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
    // 构造一个 MySqlBinlogSplit，用于在快照（snapshot splits / chunk splits）完成后进入 binlog（增量）阶段。

    // 这个 binlog split 需要携带几类关键信息：
    //	1.	从哪个 binlog offset 开始读（start offset）
    //	2.	读到哪里停止（stopping offset；snapshot-only 模式会用到）
    //	3.	每个 snapshot split 在快照完成时对应的 binlog watermark/finished offset（用于对齐快照与增量，确保一致性与不丢不重）
    //	4.	如果 metadata 太大，是否把这些 finishedSplitInfos 拆分成多个 group 后续分批下发（降低一次 RPC/状态传输压力）
    private MySqlBinlogSplit createBinlogSplit() {

        // 收集“已分配的 snapshot splits”，并排序
        // snapshotSplitAssigner.getAssignedSplits()
        //	•	这是 Assigner 维护的一个映射：splitId -> snapshotSplit
        //	•	代表“已经被分配出去（assigned）”的 snapshot splits（通常意味着已经派发给 reader 处理，是否完成要看后面的 finishedOffsets）
        //
        //为什么要 sorted(Comparator.comparing(MySqlSplit::splitId))
        //	•	为了确定性（determinism）：Map 的迭代顺序不稳定，排序后可以保证：
        //	•	生成的 finishedSnapshotSplitInfos 顺序稳定
        //	•	checkpoint / restore / 对齐逻辑更可预测（尤其在 debug / 对账时）
        //	•	排序键是 splitId，说明 splitId 是全局唯一且可比较的字符串/标识。
        final List<MySqlSchemalessSnapshotSplit> assignedSnapshotSplit =
                snapshotSplitAssigner.getAssignedSplits().values().stream()
                        .sorted(Comparator.comparing(MySqlSplit::splitId))
                        .collect(Collectors.toList());

        // 拿到每个 split 的“完成位点（finished offset）”，准备汇总
        // splitFinishedOffsets
        //	•	key：splitId
        //	•	value：该 snapshot split 完成时对应的 BinlogOffset
        //	•	这通常是快照过程中拿到的“watermark”——比如在读取某个 chunk 时，记录下“此 chunk 一致性快照对应的 binlog 位点”，以便后续增量从正确位置衔接。
        //
        //finishedSnapshotSplitInfos
        //	•	用来把每个 split 的关键信息（表、边界、finished offset）打包成结构体列表，塞给 binlog split。
        Map<String, BinlogOffset> splitFinishedOffsets =
                snapshotSplitAssigner.getSplitFinishedOffsets();
        final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos = new ArrayList<>();

        // 计算所有 snapshot splits 完成位点的最小值/最大值，并构建 FinishedSnapshotSplitInfo
        // 3.1 这段循环做两件事
        //
        //A) 统计全局 minBinlogOffset 与 maxBinlogOffset
        //	•	minBinlogOffset：所有 split 完成位点里最早的那个
        //	•	maxBinlogOffset：所有 split 完成位点里最晚的那个
        //
        //用途：
        //	•	min：作为 binlog split 的 start offset（后面会看到）
        //	•	max：在 snapshot-only 模式下作为 stopping offset（后面会看到）
        //
        //为啥 start 用 min？
        //	•	snapshot splits 是并行跑的：不同 split 完成的“watermark”可能不同。
        //	•	如果你要读增量并确保“覆盖所有快照产生的变更对齐”，通常必须从最早那个 watermark 开始读，才能保证对所有 split 的增量对齐不缺口。
        //	•	但只从 min 开始读会带来一个问题：会读到一些早于部分 split watermark 的事件 —— 这就需要 finishedSnapshotSplitInfos
        //   	来做过滤/对齐（也就是“每个 split 从自己的 watermark 开始对齐”）。所以 start=min 是“全局覆盖”，而 per-split watermark 是“精确对齐”。
        //
        //B) 组装每个 split 的 FinishedSnapshotSplitInfo
        //字段含义：
        //	•	split.getTableId()：表
        //	•	split.splitId()：split 标识
        //	•	split.getSplitStart()/getSplitEnd()：该 split 的 key 范围（chunk 边界）
        //	•	binlogOffset：该 split 的完成位点（watermark）
        //
        //这份列表在增量阶段主要用于：
        //	•	判断某条 binlog 事件应该归属哪个 split 的补偿/对齐逻辑
        //	•	在增量阶段过滤掉“早于该 split watermark 的事件”，避免重复覆盖快照已包含的数据
        //	•	或者用于生成/更新 “stream split 的 per-table/per-split offset state”
        //
        //3.2 这段代码隐含一个强假设（值得你留意）
        // 如果 splitFinishedOffsets 里缺少某个 splitId，会得到 null，随后调用：
        //	•	binlogOffset.isBefore(...) / isAfter(...)
        //会直接 NPE。
        //
        //因此，这段方法通常只会在一个前置条件满足时被调用：
        //	•	所有 assignedSnapshotSplit 的 finishedOffsets 都已写入（即 snapshot 阶段已完结且所有 split 都上报完成位点）
        //
        //如果你遇到 restart / failover 时卡在某个状态、或者 JIRA 提到的那类问题，本质上就是“状态机允许进入 createBinlogSplit，但 finishedOffsets 没齐”。
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
        // 计算 stoppingOffset：snapshot-only 模式下取 max watermark
        // 这里有两种模式：
        //
        //4.1 非 snapshot-only（即 snapshot + binlog 持续增量）
        //	•	stoppingOffset = BinlogOffset.ofNonStopping()
        //	•	语义：不停止，binlog split 会持续消费增量
        //
        //4.2 snapshot-only（只要一个一致性快照结果，不持续订阅）
        //	•	stoppingOffset = maxBinlogOffset
        //
        //为什么取 max？
        //	•	并行 snapshot splits 可能在不同时间点拿到各自的 watermark。
        //	•	如果 snapshot-only 的目标是“一个一致性快照时刻”的闭包结果，你需要把增量补偿读到一个能够覆盖所有 split watermark 的边界。
        //	•	maxBinlogOffset 是所有 split watermark 的上界：
        //	•	如果只补到某个较小值，会导致那些 watermark 更大的 splits 对齐不完整（缺少尾部变更）。
        //	•	补到 max，意味着所有 split 都能在各自 watermark 之后把增量补齐到同一个全局终点，从而“收敛到同一快照时刻”。
        //
        //注意：这并不意味着增量补偿一定会读取到“严格一致的全局时刻”，但它至少提供了一个可以把所有 split 对齐到同一个上界的机制。
        // 真正的一致性还依赖上层对 binlog 事件的分发/过滤逻辑与 checkpoint 对齐策略。
        //
        //⸻
        BinlogOffset stoppingOffset = BinlogOffset.ofNonStopping();
        if (sourceConfig.getStartupOptions().isSnapshotOnly()) {
            stoppingOffset = maxBinlogOffset;
        }

        // 元信息过大时不直接内嵌，改为后续分组下发，降低单次传输负载。
        // 元信息是否拆分分组下发（控制载荷）
        // 这句的目的非常工程化：控制一次性在 binlog split 里携带的 metadata 大小。
        //	•	finishedSnapshotSplitInfos 的长度 ≈ snapshot splits 数量
        //	•	splits 多时，这个列表可能非常大（特别是大表 + 小 chunkSize → splits 激增）
        //	•	如果每次把整份列表塞进一个 split 通过 RPC 传递/写入状态/在 checkpoint 中序列化：
        //	•	可能导致网络负载、序列化开销、状态体积暴涨
        //	•	甚至触发消息大小限制或反序列化抖动
        //
        //所以当数量超过阈值 splitMetaGroupSize 时：
        //	•	这里先不直接内嵌
        //	•	后续通过 “group meta events / 分批下发” 的方式补齐
        //	（你在代码里通常会看到类似 MetaGroup、FinishedSnapshotSplitInfo 分片发送的事件机制）
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
