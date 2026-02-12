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

package org.apache.flink.cdc.connectors.mysql.source.offset;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;

import io.debezium.connector.mysql.GtidSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * 描述 binlog 细粒度位点的数据结构，包含文件名/位置、GTID、事务内事件与行偏移等信息。
 *
 * <p>该结构不仅用于定位到某个 binlog 位置，也用于事务内精确恢复：一个事务可能包含多个 change
 * event，而一个 change event 可能对应多行数据。任务从指定 {@link BinlogOffset} 恢复时，需要跳过
 * 已处理的 event 和 row。
 */
@PublicEvolving
public class BinlogOffset implements Comparable<BinlogOffset>, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    public static final String EVENTS_TO_SKIP_OFFSET_KEY = "event";
    public static final String ROWS_TO_SKIP_OFFSET_KEY = "row";
    public static final String GTID_SET_KEY = "gtids";
    public static final String TIMESTAMP_KEY = "ts_sec";
    public static final String SERVER_ID_KEY = "server_id";
    public static final String OFFSET_KIND_KEY = "kind";

    private final Map<String, String> offset;

    // ------------------------------- Builders --------------------------------

    /** 创建 {@link BinlogOffsetBuilder}。 */
    public static BinlogOffsetBuilder builder() {
        return new BinlogOffsetBuilder();
    }

    /** 通过 binlog 文件名和 position 创建位点。 */
    public static BinlogOffset ofBinlogFilePosition(String filename, long position) {
        return builder().setBinlogFilePosition(filename, position).build();
    }

    /** 通过 GTID 集创建位点。 */
    public static BinlogOffset ofGtidSet(String gtidSet) {
        return builder().setBinlogFilePosition("", 0).setGtidSet(gtidSet).build();
    }

    /** 创建“最早可访问位点”。 */
    public static BinlogOffset ofEarliest() {
        return builder().setOffsetKind(BinlogOffsetKind.EARLIEST).build();
    }

    /** 创建“当前最新位点”（调用当下的最新位置）。 */
    public static BinlogOffset ofLatest() {
        return builder().setOffsetKind(BinlogOffsetKind.LATEST).build();
    }

    /** 通过秒级时间戳创建位点。 */
    public static BinlogOffset ofTimestampSec(long timestampSec) {
        return builder()
                .setOffsetKind(BinlogOffsetKind.TIMESTAMP)
                .setTimestampSec(timestampSec)
                .build();
    }

    @Internal
    public static BinlogOffset ofNonStopping() {
        return builder().setOffsetKind(BinlogOffsetKind.NON_STOPPING).build();
    }

    @VisibleForTesting
    public BinlogOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    // ------------------------------ 字段读取 -----------------------------

    public Map<String, String> getOffset() {
        return offset;
    }

    public String getFilename() {
        return offset.get(BINLOG_FILENAME_OFFSET_KEY);
    }

    public long getPosition() {
        return longOffsetValue(offset, BINLOG_POSITION_OFFSET_KEY);
    }

    public long getRestartSkipEvents() {
        return longOffsetValue(offset, EVENTS_TO_SKIP_OFFSET_KEY);
    }

    public long getRestartSkipRows() {
        return longOffsetValue(offset, ROWS_TO_SKIP_OFFSET_KEY);
    }

    public String getGtidSet() {
        return offset.get(GTID_SET_KEY);
    }

    public long getTimestampSec() {
        return longOffsetValue(offset, TIMESTAMP_KEY);
    }

    public Long getServerId() {
        return longOffsetValue(offset, SERVER_ID_KEY);
    }

    @Nullable
    public BinlogOffsetKind getOffsetKind() {
        if (offset.get(OFFSET_KIND_KEY) == null) {
            return null;
        }
        return BinlogOffsetKind.valueOf(offset.get(OFFSET_KIND_KEY));
    }

    private long longOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj == null) {
            return 0L;
        }
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        }
        try {
            return Long.parseLong(obj.toString());
        } catch (NumberFormatException e) {
            throw new ConnectException(
                    "Source offset '"
                            + key
                            + "' parameter value "
                            + obj
                            + " could not be converted to a long");
        }
    }

    // ---------------------- BinlogOffset 比较规则 ----------------------

    /**
     * 比较逻辑参考了
     * {@link io.debezium.relational.history.HistoryRecordComparator}。
     */
    @Override
    public int compareTo(BinlogOffset that) {
        // NON_STOPPING 视为“最大位点”。
        if (that.getOffsetKind() == BinlogOffsetKind.NON_STOPPING
                && this.getOffsetKind() == BinlogOffsetKind.NON_STOPPING) {
            return 0;
        }
        if (this.getOffsetKind() == BinlogOffsetKind.NON_STOPPING) {
            return 1;
        }
        if (that.getOffsetKind() == BinlogOffsetKind.NON_STOPPING) {
            return -1;
        }

        String gtidSetStr = this.getGtidSet();
        String targetGtidSetStr = that.getGtidSet();
        if (StringUtils.isNotEmpty(targetGtidSetStr)) {
            // 目标位点使用 GTID，优先按 GTID 语义比较。
            if (StringUtils.isNotEmpty(gtidSetStr)) {
                // 双方都有 GTID，直接比较 GTID 集关系。
                GtidSet gtidSet = new GtidSet(gtidSetStr);
                GtidSet targetGtidSet = new GtidSet(targetGtidSetStr);
                if (gtidSet.equals(targetGtidSet)) {
                    // GTID 完全相同，再用事务内 event 跳过数决定先后。
                    long restartSkipEvents = this.getRestartSkipEvents();
                    long targetRestartSkipEvents = that.getRestartSkipEvents();
                    return Long.compare(restartSkipEvents, targetRestartSkipEvents);
                }
                // GTID 不完全相等时，按“是否为对方子集”判断先后。
                return gtidSet.isContainedWithin(targetGtidSet) ? -1 : 1;
            }
            // 目标有 GTID、当前无 GTID：按“当前更早”处理（与 Debezium 语义保持一致）。
            return -1;
        } else if (StringUtils.isNotEmpty(gtidSetStr)) {
            // 当前有 GTID、目标无 GTID：按“当前更晚”处理。
            return 1;
        }

        // 双方都没有 GTID，则继续比较 serverId / 时间戳 / 文件位点。
        long serverId = this.getServerId();
        long targetServerId = that.getServerId();

        if (serverId != targetServerId) {
            // 来自不同 serverId 时，binlog 坐标不可直接比较，退化为时间戳比较。
            long timestamp = this.getTimestampSec();
            long targetTimestamp = that.getTimestampSec();
            if (timestamp != 0 && targetTimestamp != 0) {
                return Long.compare(timestamp, targetTimestamp);
            }
        }

        // 先比较 binlog 文件名。
        if (this.getFilename() != null
                && that.getFilename() != null
                && this.getFilename().compareToIgnoreCase(that.getFilename()) != 0) {
            return this.getFilename().compareToIgnoreCase(that.getFilename());
        }

        // 文件名相同，再比较 position。
        if (this.getPosition() != that.getPosition()) {
            return Long.compare(this.getPosition(), that.getPosition());
        }

        // position 相同，再比较事务内已处理 event 数。
        if (this.getRestartSkipEvents() != that.getRestartSkipEvents()) {
            return Long.compare(this.getRestartSkipEvents(), that.getRestartSkipEvents());
        }

        // event 数也相同，最后比较行偏移。
        return Long.compare(this.getRestartSkipRows(), that.getRestartSkipRows());
    }

    public boolean isAtOrBefore(BinlogOffset that) {
        return this.compareTo(that) <= 0;
    }

    public boolean isBefore(BinlogOffset that) {
        return this.compareTo(that) < 0;
    }

    public boolean isAtOrAfter(BinlogOffset that) {
        return this.compareTo(that) >= 0;
    }

    public boolean isAfter(BinlogOffset that) {
        return this.compareTo(that) > 0;
    }

    @Override
    public String toString() {
        return offset.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BinlogOffset)) {
            return false;
        }
        BinlogOffset that = (BinlogOffset) o;
        return offset.equals(that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(offset);
    }
}
