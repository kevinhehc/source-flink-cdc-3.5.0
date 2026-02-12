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

/**
 * Binlog 偏移量的预定义类型。
 *
 * <p>该枚举用于表达“如何定位 binlog 起止位置”这类语义，而不只是单一的 file/pos 数值。
 * 例如：从最早可读位置开始、从当前最新位置开始、按时间戳定位，或使用精确位点（file+pos / GTID）。
 */
public enum BinlogOffsetKind {

    /** 最早可访问的 binlog 位点。常用于“全量后接增量”时从历史最早位置开始读。 */
    EARLIEST,

    /** 当前最新的 binlog 位点。常用于仅消费新产生变更（不回放历史）。 */
    LATEST,

    /**
     * 以时间戳描述位点。
     *
     * <p>例如 "1667232000" 表示从 2022-11-01 00:00:00 之后的事件开始读取。
     */
    TIMESTAMP,

    /**
     * 精确位点：由 binlog 文件名+position 描述；若集群开启 GTID，也可由 GTID 集描述。示例：
     *
     * <ul>
     *   <li>binlog 文件 "mysql-bin.000002"，position 为 4
     *   <li>GTID 集 "24DA167-0C0C-11E8-8442-00059A3C7B00:1-19"
     * </ul>
     */
    SPECIFIC,

    /**
     * 特殊位点：表示 binlog 读取没有结束位点（持续读取）。
     *
     * <p>注意：这是内部语义（INTERNAL），不用于指定“起始位点”。
     */
    NON_STOPPING
}
