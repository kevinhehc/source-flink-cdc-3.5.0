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

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * 描述 chunk 区间的内部结构：chunkStart（含）到 chunkEnd（不含）。
 *
 * <p>其中 {@code null} 表示无界起点/终点。
 */
class ChunkRange {
    private final @Nullable Object chunkStart;
    private final @Nullable Object chunkEnd;

    /**
     * 返回表示“全表扫描”的 {@link ChunkRange}（起止边界都无界）。
     */
    public static ChunkRange all() {
        return new ChunkRange(null, null);
    }

    /** 使用给定起止边界创建 {@link ChunkRange}。 */
    public static ChunkRange of(Object chunkStart, Object chunkEnd) {
        return new ChunkRange(chunkStart, chunkEnd);
    }

    private ChunkRange(@Nullable Object chunkStart, @Nullable Object chunkEnd) {
        if (chunkStart != null || chunkEnd != null) {
            checkArgument(
                    !Objects.equals(chunkStart, chunkEnd),
                    "Chunk start %s shouldn't be equal to chunk end %s",
                    chunkStart,
                    chunkEnd);
        }
        this.chunkStart = chunkStart;
        this.chunkEnd = chunkEnd;
    }

    @Nullable
    public Object getChunkStart() {
        return chunkStart;
    }

    @Nullable
    public Object getChunkEnd() {
        return chunkEnd;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ChunkRange that = (ChunkRange) o;
        return Objects.equals(chunkStart, that.chunkStart)
                && Objects.equals(chunkEnd, that.chunkEnd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunkStart, chunkEnd);
    }

    @Override
    public String toString() {
        return "ChunkRange{chunkStart=" + chunkStart + ", chunkEnd=" + chunkEnd + '}';
    }
}
