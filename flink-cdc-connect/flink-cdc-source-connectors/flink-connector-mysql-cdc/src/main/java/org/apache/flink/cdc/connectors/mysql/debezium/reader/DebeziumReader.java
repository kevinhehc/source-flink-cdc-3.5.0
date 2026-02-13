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

import javax.annotation.Nullable;

import java.util.Iterator;

/** 读取表 split 的 Reader，split 可以是 snapshot split 或 binlog split。 */
public interface DebeziumReader<T, Split> {

    /** 返回当前 split 是否读取完成。 */
    boolean isFinished();

    /**
     * 提交一个待读取的 split，仅应在 reader 空闲时调用。
     *
     * @param splitToRead 待读取 split
     */
    void submitSplit(Split splitToRead);

    /** 关闭 reader 并释放所有资源。 */
    void close();

    /**
     * 从 MySQL 拉取记录。
     *
     * <p>到达 split 末尾时返回 null；若当前正在拉取、暂时没有可返回数据，则返回空的 {@link Iterator}。
     */
    @Nullable
    Iterator<T> pollSplitRecords() throws InterruptedException;
}
