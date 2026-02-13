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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

/**
 * split assigner 的有限状态机状态。
 *
 * <p>这里使用 status 而不是 state，避免与 Flink 的 state 概念混淆。状态流转如下：
 *
 * <pre>
 *        INITIAL_ASSIGNING(start)
 *              |
 *              |
 *          onFinish()
 *              |
 *              ↓
 *    INITIAL_ASSIGNING_FINISHED(end)
 *              |
 *              |
 *      startAssignNewlyTables() // found newly added tables, assign newly added tables
 *              |
 *              ↓
 *     NEWLY_ADDED_ASSIGNING ---onFinish()--→ NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED---onBinlogSplitUpdated()---> NEWLY_ADDED_ASSIGNING_FINISHED(end)
 *              ↑                                                                                                               |
 *              |                                                                                                               |
 *              |--------------- startAssignNewlyTables() //found newly added tables, assign newly added tables ----------------|
 * </pre>
 */
public enum AssignerStatus {
    INITIAL_ASSIGNING(0) {
        @Override
        public AssignerStatus getNextStatus() {
            return INITIAL_ASSIGNING_FINISHED;
        }

        @Override
        public AssignerStatus onFinish() {
            LOG.info(
                    "Assigner status changes from INITIAL_ASSIGNING to INITIAL_ASSIGNING_FINISHED");
            return this.getNextStatus();
        }
    },
    INITIAL_ASSIGNING_FINISHED(1) {
        @Override
        public AssignerStatus getNextStatus() {
            return NEWLY_ADDED_ASSIGNING;
        }

        @Override
        public AssignerStatus startAssignNewlyTables() {
            LOG.info(
                    "Assigner status changes from INITIAL_ASSIGNING_FINISHED to NEW_ADDED_ASSIGNING");
            return this.getNextStatus();
        }
    },
    NEWLY_ADDED_ASSIGNING(2) {
        @Override
        public AssignerStatus getNextStatus() {
            return NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED;
        }

        @Override
        public AssignerStatus onFinish() {
            LOG.info(
                    "Assigner status changes from NEWLY_ADDED_ASSIGNING to NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED");
            return this.getNextStatus();
        }
    },
    NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED(3) {
        @Override
        public AssignerStatus getNextStatus() {
            return NEWLY_ADDED_ASSIGNING_FINISHED;
        }

        @Override
        public AssignerStatus onBinlogSplitUpdated() {
            LOG.info(
                    "Assigner status changes from NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED to NEWLY_ADDED_ASSIGNING_FINISHED");
            return this.getNextStatus();
        }
    },
    NEWLY_ADDED_ASSIGNING_FINISHED(4) {
        @Override
        public AssignerStatus getNextStatus() {
            return NEWLY_ADDED_ASSIGNING;
        }

        @Override
        public AssignerStatus startAssignNewlyTables() {
            LOG.info(
                    "Assigner status changes from NEWLY_ADDED_ASSIGNING_FINISHED to NEWLY_ADDED_ASSIGNING");
            return this.getNextStatus();
        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(AssignerStatus.class);
    private final int statusCode;

    AssignerStatus(int statusCode) {
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public abstract AssignerStatus getNextStatus();

    public AssignerStatus onFinish() {
        throw new IllegalStateException(
                format(
                        "Invalid call, assigner under %s state can not call onFinish()",
                        fromStatusCode(this.getStatusCode())));
    }

    public AssignerStatus startAssignNewlyTables() {
        throw new IllegalStateException(
                format(
                        "Invalid call, assigner under %s state can not call startAssignNewlyTables()",
                        fromStatusCode(this.getStatusCode())));
    }

    public AssignerStatus onBinlogSplitUpdated() {
        throw new IllegalStateException(
                format(
                        "Invalid call, assigner under %s state can not call onBinlogSplitUpdated()",
                        fromStatusCode(this.getStatusCode())));
    }

    // --------------------------------------------------------------------------------------------
    // 工具方法
    // --------------------------------------------------------------------------------------------

    /** 根据状态码获取 {@link AssignerStatus}。 */
    public static AssignerStatus fromStatusCode(int statusCode) {
        switch (statusCode) {
            case 0:
                // 含义：初始抓取阶段（initial tables）正在分配快照 splits。 ￼
                //
                //典型发生在：
                //	•	Job 第一次启动，StartupMode=initial / 或者需要 snapshot 的模式；
                //	•	Assigner 正在：
                //	•	枚举捕获表（captured tables）
                //	•	选择 split key（通常是 PK/唯一键）
                //	•	把每张表切成多个 snapshot splits（chunks） 并持续 getNext() 分发给 readers
                //
                //你可以把它理解为：“还在给‘初始就配置好的那些表’派发全量 chunk 任务”。
                return INITIAL_ASSIGNING;
            case 1:
                // 含义：初始表的快照 splits 已经“分配完成/快照阶段结束”，初始分配阶段到达 end。 ￼
                //
                //这里的 “FINISHED” 在 Flink CDC 的语义里通常不只是“我不再产生新的 snapshot split 了”，
                // 还隐含了“快照 splits 在 pipeline 里已被完整处理到可以进入后续阶段”
                // （Javadoc 对 isSnapshotAssigningFinished 的说明强调了：没有更多 snapshot splits，且 splits 的记录已在 pipeline 中被完全处理）。 ￼
                //
                //该状态的关键效果：
                //	•	Assigner 认为 initial snapshot 部分已经收口，可以进入后续逻辑（例如进入持续 binlog / stream split 读取，或者允许检查新增表）。
                //
                //你在排障时可以把它当成：“初始快照切分/派发这一段收工了”。
                return INITIAL_ASSIGNING_FINISHED;
            case 2:
                // 含义：开始处理“新增表”的分配阶段。 ￼
                //
                //它不是每个作业都会出现, 只有当你开启了类似“允许运行中发现并纳入新表”的能力（例如配置支持 newly added tables）时，
                // 才会从 INITIAL_ASSIGNING_FINISHED 通过 startAssignNewlyTables() 进入这个状态。 ￼
                //
                //这时 Assigner 做的事情本质上和 initial snapshot 很像，但对象变了：
                //	•	initial 表都处理完了；
                //	•	发现有新表（比如你更新了 table list、或者 schema/table 变更被纳入捕获范围）；
                //	•	对“新表集合”切 snapshot splits 并分发。
                //
                //你可以把它理解为：“在跑增量的同时，插入了一段‘补新表历史数据’的快照切分/派发”。
                return NEWLY_ADDED_ASSIGNING;
            case 3:
                // 含义：新增表的 snapshot splits 阶段结束（新增表的快照 chunk 已经处理完），但还没完成“流式 split 更新”。 ￼
                //
                //为什么会有这个“中间态”？
                //	•	对新增表来说，做完 snapshot 只是第一步；
                //	•	你还需要把“新增表快照完成时刻”对应的 binlog/stream split 元信息更新进去（保证后续持续读 binlog 时不会漏新表、也不会重复/乱序）。
                //
                //Jira 的一个真实问题就明确提到：作业 stop/restart 后状态停在 NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED，
                // 随后 Enumerator 会触发与 readers 的同步，发起 binlog split 更新/元信息请求；如果此时 finished split infos 还没准备好就可能报错。 ￼
                //
                //因此这个状态可以理解为：
                //	•	“新表快照已完成”
                //	•	“正在等待把 stream/binlog split 更新到包含新表的正确边界/元信息”
                return NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED;
            case 4:
                // 含义：新增表处理的整个闭环结束（包括：新表 snapshot 完成 + stream/binlog split 更新完成），到达 end。 ￼
                //
                //它和状态 1 的“finished”类似，代表一个阶段收口；区别在于：
                //	•	INITIAL_ASSIGNING_FINISHED：收口的是“初始表”快照阶段；
                //	•	NEWLY_ADDED_ASSIGNING_FINISHED：收口的是“一轮新增表”的补数闭环（快照 + 流式更新）。
                //
                //并且它可以再次进入下一轮 NEWLY_ADDED_ASSIGNING（如果后续又发现新表）。 ￼
                //
                //另一个 Jira 提到：Enumerator 在过滤 snapshot splits 时，只在 INITIAL_ASSIGNING_FINISHED
                // 或 NEWLY_ADDED_ASSIGNING_FINISHED 才认为“assigner 已经 finished（可过滤/可切阶段）”，也从侧面说明这两个状态是“阶段真正收口”的标志位。
                return NEWLY_ADDED_ASSIGNING_FINISHED;
            default:
                throw new IllegalStateException(
                        format(
                                "Invalid status code %s,the valid code range is [0, 4]",
                                statusCode));
        }
    }

    /**
     * 判断是否已完成 snapshot split 的分配。
     *
     * <p>返回 true 表示当前不再有待分配 snapshot split，且相关 split 记录已在流水线中处理完成。
     */
    public static boolean isSnapshotAssigningFinished(AssignerStatus assignerStatus) {
        return assignerStatus == INITIAL_ASSIGNING_FINISHED
                || assignerStatus == NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED;
    }

    /**
     * 判断是否已完成当前轮次所有 split 分配。
     *
     * <p>返回 true 时，表示可以进入/继续新增表分配流程。
     */
    public static boolean isAssigningFinished(AssignerStatus assignerStatus) {
        return assignerStatus == INITIAL_ASSIGNING_FINISHED
                || assignerStatus == NEWLY_ADDED_ASSIGNING_FINISHED;
    }

    /** 判断当前是否处于 snapshot split 分配中。 */
    public static boolean isAssigningSnapshotSplits(AssignerStatus assignerStatus) {
        return assignerStatus == INITIAL_ASSIGNING || assignerStatus == NEWLY_ADDED_ASSIGNING;
    }

    /** 判断是否已完成初始表分配。 */
    public static boolean isInitialAssigningFinished(AssignerStatus assignerStatus) {
        return assignerStatus == INITIAL_ASSIGNING_FINISHED;
    }

    /** 判断是否已完成新增表分配。 */
    public static boolean isNewlyAddedAssigningFinished(AssignerStatus assignerStatus) {
        return assignerStatus == NEWLY_ADDED_ASSIGNING_FINISHED;
    }

    /**
     * 判断是否已完成“新增表的 snapshot split”分配阶段。
     */
    public static boolean isNewlyAddedAssigningSnapshotFinished(AssignerStatus assignerStatus) {
        return assignerStatus == NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED;
    }
}
