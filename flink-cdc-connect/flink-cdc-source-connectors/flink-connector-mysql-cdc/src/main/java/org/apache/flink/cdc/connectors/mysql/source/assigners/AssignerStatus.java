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
                return INITIAL_ASSIGNING;
            case 1:
                return INITIAL_ASSIGNING_FINISHED;
            case 2:
                return NEWLY_ADDED_ASSIGNING;
            case 3:
                return NEWLY_ADDED_ASSIGNING_SNAPSHOT_FINISHED;
            case 4:
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
