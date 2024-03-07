/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test implementation for testing restore job related stats captured
 */
public class TestRestoreJobStats implements RestoreJobStats
{
    public final List<Long> sliceCompletionTimes = new ArrayList<>();
    public final List<Long> sliceImportTimes = new ArrayList<>();
    public final List<Long> sliceUnzipTimes = new ArrayList<>();
    public final List<Long> sliceValidationTimes = new ArrayList<>();
    public final List<Long> sliceDownloadTimes = new ArrayList<>();
    public final List<Long> sliceReplicationTimes = new ArrayList<>();
    public final List<Long> sliceImportQueueLengths = new ArrayList<>();
    public final List<Long> pendingSliceCounts = new ArrayList<>();
    public final List<Long> sstableDataComponentSizes = new ArrayList<>();
    public long successJobCount;
    public long failedJobCount;
    public long activeJobCount;
    public long tokenRefreshCount;
    public Map<Integer, Long> longRunningRestoreHandlers = new HashMap<>();

    @Override
    public void captureSliceCompletionTime(int instanceId, long durationNanos)
    {
        sliceCompletionTimes.add(durationNanos);
    }

    @Override
    public void captureSliceImportTime(int instanceId, long durationNanos)
    {
        sliceImportTimes.add(durationNanos);
    }

    @Override
    public void captureSliceUnzipTime(int instanceId, long durationNanos)
    {
        sliceUnzipTimes.add(durationNanos);
    }

    @Override
    public void captureSliceValidationTime(int instanceId, long durationNanos)
    {
        sliceValidationTimes.add(durationNanos);
    }

    @Override
    public void captureSliceDownloaded(int instanceId, long compressedSizeInBytes, long uncompressedSizeInBytes,
                                       long durationNanos)
    {
        sliceDownloadTimes.add(durationNanos);
    }

    @Override
    public void captureSliceReplicationTime(long durationNanos)
    {
        sliceReplicationTimes.add(durationNanos);
    }

    @Override
    public void captureSliceImportQueueLength(int instanceId, long length)
    {
        sliceImportQueueLengths.add(length);
    }

    @Override
    public void capturePendingSliceCount(int instanceId, long count)
    {
        pendingSliceCounts.add(count);
    }

    @Override
    public void captureSSTableDataComponentSize(int instanceId, long sizeInBytes)
    {
        sstableDataComponentSizes.add(sizeInBytes);
    }

    @Override
    public void captureSuccessJob()
    {
        successJobCount += 1;
    }

    @Override
    public void captureFailedJob()
    {
        failedJobCount += 1;
    }

    @Override
    public void captureActiveJobs(long count)
    {
        activeJobCount = count;
    }

    @Override
    public void captureTokenRefreshed()
    {
        tokenRefreshCount += 1;
    }

    public void captureLongRunningRestoreHandler(int instanceId, long handlerDuration)
    {
        longRunningRestoreHandlers.put(instanceId, handlerDuration);
    }
}
