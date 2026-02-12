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

package org.apache.flink.cdc.connectors.mysql.source.split;

import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;

import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges.TableChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** 描述 MySQL 表（单表或多表）binlog 范围的 split。 */
public class MySqlBinlogSplit extends MySqlSplit {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlBinlogSplit.class);
    private static final int TABLES_LENGTH_FOR_LOG = 3;

    private final BinlogOffset startingOffset;
    private final BinlogOffset endingOffset;
    private final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos;
    private final Map<TableId, TableChange> tableSchemas;
    private final int totalFinishedSplitSize;
    private final boolean isSuspended;
    private final String tablesForLog;
    @Nullable transient byte[] serializedFormCache;

    public MySqlBinlogSplit(
            String splitId,
            BinlogOffset startingOffset,
            BinlogOffset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas,
            int totalFinishedSplitSize,
            boolean isSuspended) {
        super(splitId);
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.finishedSnapshotSplitInfos = finishedSnapshotSplitInfos;
        this.tableSchemas = tableSchemas;
        this.totalFinishedSplitSize = totalFinishedSplitSize;
        this.isSuspended = isSuspended;
        this.tablesForLog = getTablesForLog();
    }

    public MySqlBinlogSplit(
            String splitId,
            BinlogOffset startingOffset,
            BinlogOffset endingOffset,
            List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos,
            Map<TableId, TableChange> tableSchemas,
            int totalFinishedSplitSize) {
        super(splitId);
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.finishedSnapshotSplitInfos = finishedSnapshotSplitInfos;
        this.tableSchemas = tableSchemas;
        this.totalFinishedSplitSize = totalFinishedSplitSize;
        this.isSuspended = false;
        this.tablesForLog = getTablesForLog();
    }

    public BinlogOffset getStartingOffset() {
        return startingOffset;
    }

    public BinlogOffset getEndingOffset() {
        return endingOffset;
    }

    public List<FinishedSnapshotSplitInfo> getFinishedSnapshotSplitInfos() {
        return finishedSnapshotSplitInfos;
    }

    @Override
    public Map<TableId, TableChange> getTableSchemas() {
        return tableSchemas;
    }

    public int getTotalFinishedSplitSize() {
        return totalFinishedSplitSize;
    }

    public boolean isSuspended() {
        return isSuspended;
    }

    public boolean isCompletedSplit() {
        return totalFinishedSplitSize == finishedSnapshotSplitInfos.size();
    }

    private String getTablesForLog() {
        List<TableId> tablesForLog = new ArrayList<>();
        if (tableSchemas != null) {
            List<TableId> tableIds = new ArrayList<>(new TreeSet(tableSchemas.keySet()));
            // 截断表名数量，避免日志过长。
            tablesForLog = tableIds.subList(0, Math.min(tableIds.size(), TABLES_LENGTH_FOR_LOG));
        }
        return tablesForLog.toString();
    }

    public String getTables() {
        return tablesForLog;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MySqlBinlogSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MySqlBinlogSplit that = (MySqlBinlogSplit) o;
        return totalFinishedSplitSize == that.totalFinishedSplitSize
                && isSuspended == that.isSuspended
                && Objects.equals(startingOffset, that.startingOffset)
                && Objects.equals(endingOffset, that.endingOffset)
                && Objects.equals(finishedSnapshotSplitInfos, that.finishedSnapshotSplitInfos)
                && Objects.equals(tableSchemas, that.tableSchemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                startingOffset,
                endingOffset,
                finishedSnapshotSplitInfos,
                tableSchemas,
                totalFinishedSplitSize,
                isSuspended);
    }

    @Override
    public String toString() {
        return "MySqlBinlogSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", tables="
                + tablesForLog
                + ", offset="
                + startingOffset
                + ", endOffset="
                + endingOffset
                + ", isSuspended="
                + isSuspended
                + '}';
    }

    // -------------------------------------------------------------------
    // 构建新 MySqlBinlogSplit 实例的工厂方法
    // -------------------------------------------------------------------
    public static MySqlBinlogSplit appendFinishedSplitInfos(
            MySqlBinlogSplit binlogSplit, List<FinishedSnapshotSplitInfo> splitInfos) {
        // 新增表后，重新计算起始 binlog offset（取更早的 high watermark）。
        BinlogOffset startingOffset = binlogSplit.getStartingOffset();
        for (FinishedSnapshotSplitInfo splitInfo : splitInfos) {
            if (splitInfo.getHighWatermark().isBefore(startingOffset)) {
                startingOffset = splitInfo.getHighWatermark();
            }
        }
        splitInfos.addAll(binlogSplit.getFinishedSnapshotSplitInfos());
        return new MySqlBinlogSplit(
                binlogSplit.splitId,
                startingOffset,
                binlogSplit.getEndingOffset(),
                splitInfos,
                binlogSplit.getTableSchemas(),
                binlogSplit.getTotalFinishedSplitSize(),
                binlogSplit.isSuspended());
    }

    /**
     * 过滤 {@link MySqlBinlogSplit} 中过期的 finished split 信息。
     *
     * <p>从 checkpoint 恢复时，finished split 列表可能包含已删除表的 split。这里会将这些 split 从
     * finished 列表中移除并更新数量，同时从 binlog split 的 tableSchemas 中移除对应表。
     */
    public static MySqlBinlogSplit filterOutdatedSplitInfos(
            MySqlBinlogSplit binlogSplit, Tables.TableFilter currentTableFilter) {
        Map<TableId, TableChange> filteredTableSchemas =
                binlogSplit.getTableSchemas().entrySet().stream()
                        .filter(entry -> currentTableFilter.isIncluded(entry.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Set<TableId> tablesToRemoveInFinishedSnapshotSplitInfos =
                binlogSplit.getFinishedSnapshotSplitInfos().stream()
                        .filter(i -> !currentTableFilter.isIncluded(i.getTableId()))
                        .map(split -> split.getTableId())
                        .collect(Collectors.toSet());
        if (tablesToRemoveInFinishedSnapshotSplitInfos.isEmpty()) {
            return new MySqlBinlogSplit(
                    binlogSplit.splitId,
                    binlogSplit.getStartingOffset(),
                    binlogSplit.getEndingOffset(),
                    binlogSplit.getFinishedSnapshotSplitInfos(),
                    filteredTableSchemas,
                    binlogSplit.totalFinishedSplitSize,
                    binlogSplit.isSuspended());
        }

        LOG.info(
                "Reader remove tables after restart: {}",
                tablesToRemoveInFinishedSnapshotSplitInfos);
        List<FinishedSnapshotSplitInfo> allFinishedSnapshotSplitInfos =
                binlogSplit.getFinishedSnapshotSplitInfos().stream()
                        .filter(
                                i ->
                                        !tablesToRemoveInFinishedSnapshotSplitInfos.contains(
                                                i.getTableId()))
                        .collect(Collectors.toList());
        return new MySqlBinlogSplit(
                binlogSplit.splitId,
                binlogSplit.getStartingOffset(),
                binlogSplit.getEndingOffset(),
                allFinishedSnapshotSplitInfos,
                filteredTableSchemas,
                binlogSplit.getTotalFinishedSplitSize()
                        - (binlogSplit.getFinishedSnapshotSplitInfos().size()
                                - allFinishedSnapshotSplitInfos.size()),
                binlogSplit.isSuspended());
    }

    public static MySqlBinlogSplit fillTableSchemas(
            MySqlBinlogSplit binlogSplit, Map<TableId, TableChange> tableSchemas) {
        tableSchemas.putAll(binlogSplit.getTableSchemas());
        return new MySqlBinlogSplit(
                binlogSplit.splitId,
                binlogSplit.getStartingOffset(),
                binlogSplit.getEndingOffset(),
                binlogSplit.getFinishedSnapshotSplitInfos(),
                tableSchemas,
                binlogSplit.getTotalFinishedSplitSize(),
                binlogSplit.isSuspended());
    }

    public static MySqlBinlogSplit toNormalBinlogSplit(
            MySqlBinlogSplit suspendedBinlogSplit, int totalFinishedSplitSize) {
        return new MySqlBinlogSplit(
                suspendedBinlogSplit.splitId,
                suspendedBinlogSplit.getStartingOffset(),
                suspendedBinlogSplit.getEndingOffset(),
                suspendedBinlogSplit.getFinishedSnapshotSplitInfos(),
                suspendedBinlogSplit.getTableSchemas(),
                totalFinishedSplitSize,
                false);
    }

    public static MySqlBinlogSplit toSuspendedBinlogSplit(MySqlBinlogSplit normalBinlogSplit) {
        return new MySqlBinlogSplit(
                normalBinlogSplit.splitId,
                normalBinlogSplit.getStartingOffset(),
                normalBinlogSplit.getEndingOffset(),
                forwardHighWatermarkToStartingOffset(
                        normalBinlogSplit.getFinishedSnapshotSplitInfos(),
                        normalBinlogSplit.getStartingOffset()),
                normalBinlogSplit.getTableSchemas(),
                normalBinlogSplit.getTotalFinishedSplitSize(),
                true);
    }

    /**
     * 将已开始读取 binlog 的 snapshot split 的 high watermark 前推到当前 binlog 读取位点。
     *
     * <p>这对“新增表”场景很关键：可以让这些 split 从更新后的 high watermark 继续消费，避免重复回读。
     *
     * @param existedSplitInfos 现有 finished snapshot split 信息
     * @param currentBinlogReadingOffset 当前 binlog 读取位点
     */
    private static List<FinishedSnapshotSplitInfo> forwardHighWatermarkToStartingOffset(
            List<FinishedSnapshotSplitInfo> existedSplitInfos,
            BinlogOffset currentBinlogReadingOffset) {
        List<FinishedSnapshotSplitInfo> updatedSnapshotSplitInfos = new ArrayList<>();
        for (FinishedSnapshotSplitInfo existedSplitInfo : existedSplitInfos) {
            // 对已开始读取 binlog 的 split，将其 high watermark 前推到当前读取位点。
            if (existedSplitInfo.getHighWatermark().isBefore(currentBinlogReadingOffset)) {
                FinishedSnapshotSplitInfo forwardHighWatermarkSnapshotSplitInfo =
                        new FinishedSnapshotSplitInfo(
                                existedSplitInfo.getTableId(),
                                existedSplitInfo.getSplitId(),
                                existedSplitInfo.getSplitStart(),
                                existedSplitInfo.getSplitEnd(),
                                currentBinlogReadingOffset);
                updatedSnapshotSplitInfos.add(forwardHighWatermarkSnapshotSplitInfo);
            } else {
                updatedSnapshotSplitInfos.add(existedSplitInfo);
            }
        }
        return updatedSnapshotSplitInfos;
    }
}
