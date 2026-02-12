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

package org.apache.flink.cdc.connectors.mysql.source.assigners.state;

import org.apache.flink.cdc.connectors.mysql.source.assigners.AssignerStatus;
import org.apache.flink.cdc.connectors.mysql.source.assigners.ChunkSplitter;
import org.apache.flink.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSchemalessSnapshotSplit;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** snapshot 阶段待分配 split 的 {@link PendingSplitsState}。 */
public class SnapshotPendingSplitsState extends PendingSplitsState {

    /** checkpoint 中尚未处理完成的表。 */
    private final List<TableId> remainingTables;

    /**
     * 不再出现在 enumerator checkpoint 中、但历史上已经处理过的表。
     *
     * <p>这类表在后续恢复时应被忽略，仅对持续监控模式生效。
     */
    private final List<TableId> alreadyProcessedTables;

    /** checkpoint 中尚未分配/处理完成的 snapshot splits。 */
    private final List<MySqlSchemalessSnapshotSplit> remainingSplits;

    /**
     * 已由 {@link MySqlSourceEnumerator} 分配给 {@link MySqlSplitReader} 的 snapshot splits。
     */
    private final Map<String, MySqlSchemalessSnapshotSplit> assignedSplits;

    /**
     * {@link MySqlSourceEnumerator} 从 {@link MySqlSplitReader} 收到的已完成 split 对应 offset。
     */
    private final Map<String, BinlogOffset> splitFinishedOffsets;

    /** 表示 snapshot assigner 当前阶段的 {@link AssignerStatus}。 */
    private final AssignerStatus assignerStatus;

    /** 表标识符是否大小写敏感。 */
    private final boolean isTableIdCaseSensitive;

    /** 做 snapshot 状态快照时是否保留 remainingTables。 */
    private final boolean isRemainingTablesCheckpointed;

    private final Map<TableId, TableChange> tableSchemas;

    /** 记录 {@link ChunkSplitter} 进度的状态结构。 */
    private final ChunkSplitterState chunkSplitterState;

    public SnapshotPendingSplitsState(
            List<TableId> alreadyProcessedTables,
            List<MySqlSchemalessSnapshotSplit> remainingSplits,
            Map<String, MySqlSchemalessSnapshotSplit> assignedSplits,
            Map<TableId, TableChange> tableSchemas,
            Map<String, BinlogOffset> splitFinishedOffsets,
            AssignerStatus assignerStatus,
            List<TableId> remainingTables,
            boolean isTableIdCaseSensitive,
            boolean isRemainingTablesCheckpointed,
            ChunkSplitterState chunkSplitterState) {
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.remainingSplits = remainingSplits;
        this.assignedSplits = assignedSplits;
        this.splitFinishedOffsets = splitFinishedOffsets;
        this.assignerStatus = assignerStatus;
        this.remainingTables = remainingTables;
        this.isTableIdCaseSensitive = isTableIdCaseSensitive;
        this.isRemainingTablesCheckpointed = isRemainingTablesCheckpointed;
        this.tableSchemas = tableSchemas;
        this.chunkSplitterState = chunkSplitterState;
    }

    public List<TableId> getAlreadyProcessedTables() {
        return alreadyProcessedTables;
    }

    public List<MySqlSchemalessSnapshotSplit> getRemainingSplits() {
        return remainingSplits;
    }

    public Map<String, MySqlSchemalessSnapshotSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public Map<TableId, TableChange> getTableSchemas() {
        return tableSchemas;
    }

    public Map<String, BinlogOffset> getSplitFinishedOffsets() {
        return splitFinishedOffsets;
    }

    public AssignerStatus getSnapshotAssignerStatus() {
        return assignerStatus;
    }

    public List<TableId> getRemainingTables() {
        return remainingTables;
    }

    public boolean isTableIdCaseSensitive() {
        return isTableIdCaseSensitive;
    }

    public boolean isRemainingTablesCheckpointed() {
        return isRemainingTablesCheckpointed;
    }

    public ChunkSplitterState getChunkSplitterState() {
        return chunkSplitterState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SnapshotPendingSplitsState)) {
            return false;
        }
        SnapshotPendingSplitsState that = (SnapshotPendingSplitsState) o;
        return assignerStatus == that.assignerStatus
                && isTableIdCaseSensitive == that.isTableIdCaseSensitive
                && isRemainingTablesCheckpointed == that.isRemainingTablesCheckpointed
                && Objects.equals(remainingTables, that.remainingTables)
                && Objects.equals(alreadyProcessedTables, that.alreadyProcessedTables)
                && Objects.equals(remainingSplits, that.remainingSplits)
                && Objects.equals(assignedSplits, that.assignedSplits)
                && Objects.equals(splitFinishedOffsets, that.splitFinishedOffsets)
                && Objects.equals(chunkSplitterState, that.chunkSplitterState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                remainingTables,
                alreadyProcessedTables,
                remainingSplits,
                assignedSplits,
                splitFinishedOffsets,
                assignerStatus,
                isTableIdCaseSensitive,
                isRemainingTablesCheckpointed,
                chunkSplitterState);
    }

    @Override
    public String toString() {
        return "SnapshotPendingSplitsState{"
                + "remainingTables="
                + remainingTables
                + ", alreadyProcessedTables="
                + alreadyProcessedTables
                + ", remainingSplits="
                + remainingSplits
                + ", assignedSplits="
                + assignedSplits
                + ", splitFinishedOffsets="
                + splitFinishedOffsets
                + ", assignerStatus="
                + assignerStatus
                + ", isTableIdCaseSensitive="
                + isTableIdCaseSensitive
                + ", isRemainingTablesCheckpointed="
                + isRemainingTablesCheckpointed
                + ", chunkSplitterState="
                + chunkSplitterState
                + '}';
    }
}
