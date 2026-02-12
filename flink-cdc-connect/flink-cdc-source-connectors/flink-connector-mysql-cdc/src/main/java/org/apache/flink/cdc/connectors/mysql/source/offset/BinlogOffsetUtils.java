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

import org.apache.flink.cdc.connectors.mysql.debezium.DebeziumUtils;
import org.apache.flink.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;

import io.debezium.connector.mysql.MySqlConnection;

/** {@link BinlogOffset} 处理工具类。 */
public class BinlogOffsetUtils {

    /**
     * 根据位点类型初始化“生效位点”，让 Debezium reader 能够正确解析并 seek 到目标位置。
     *
     * <p>该方法用于 binlog 读取阶段，在初始化 {@link StatefulTaskContext} 时计算真实可用的读取位点。
     *
     * <p>初始化完成后，位点语义会落到可执行的“具体位点”（SPECIFIC），因为最终都需要转换为可定位的
     * binlog 精确位置。
     *
     * <p>初始化策略：
     *
     * <ul>
     *   <li>EARLIEST：file="", position=0
     *   <li>TIMESTAMP：通过时间戳查找对应位点（毫秒），从该时刻附近开始消费
     *   <li>LATEST：通过 JDBC 拉取当前最新位点
     * </ul>
     */
    public static BinlogOffset initializeEffectiveOffset(
            BinlogOffset offset, MySqlConnection connection, MySqlSourceConfig mySqlSourceConfig) {
        BinlogOffsetKind offsetKind = offset.getOffsetKind();
        switch (offsetKind) {
            case EARLIEST:
                return BinlogOffset.ofBinlogFilePosition("", 0);
            case TIMESTAMP:
                return DebeziumUtils.findBinlogOffset(
                        offset.getTimestampSec() * 1000, connection, mySqlSourceConfig);
            case LATEST:
                return DebeziumUtils.currentBinlogOffset(connection);
            default:
                return offset;
        }
    }

    /** 判断给定位点是否为“持续读取、不设结束位点”的内部语义。 */
    public static boolean isNonStoppingOffset(BinlogOffset binlogOffset) {
        return BinlogOffsetKind.NON_STOPPING.equals(binlogOffset.getOffsetKind());
    }
}
