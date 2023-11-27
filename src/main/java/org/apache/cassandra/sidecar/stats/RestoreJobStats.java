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

/**
 * Interface to collect all statistics related to restore feature of Sidecar
 */
public interface RestoreJobStats
{
    /**
     * Captures the total time taken to complete a slice successfully
     *
     * @param instanceId    instance that contains the slice
     * @param durationNanos duration in nanoseconds
     */
    default void captureSliceCompletionTime(int instanceId, long durationNanos)
    {

    }

    /**
     * Captures the time taken to import SSTable(s) in a slice
     *
     * @param instanceId    instance that owns the slice
     * @param durationNanos duration in nanoseconds
     */
    default void captureSliceImportTime(int instanceId, long durationNanos)
    {

    }

    /**
     * Capture the time taken to unzip a slice
     *
     * @param instanceId    instance that owns the slice
     * @param durationNanos duration in nanoseconds
     */
    default void captureSliceUnzipTime(int instanceId, long durationNanos)
    {

    }

    /**
     * Captures the time taken to validate the files in a slice
     *
     * @param instanceId    instance that owns the slice
     * @param durationNanos duration in nanoseconds
     */
    default void captureSliceValidationTime(int instanceId, long durationNanos)
    {

    }

    /**
     * Captures the throughput, concurrency and duration of downloads from s3,
     * the data size of slices, both compressed and uncompressed, in bytes
     *
     * @param instanceId              instance that owns the slice
     * @param compressedSizeInBytes   the size of the slice in s3
     * @param uncompressedSizeInBytes the total size of the files included in the slice
     * @param durationNanos           duration in nanoseconds
     */
    default void captureSliceDownloaded(int instanceId,
                                        long compressedSizeInBytes,
                                        long uncompressedSizeInBytes,
                                        long durationNanos)
    {

    }

    /**
     * Captures slice S3 download timeout
     *
     * @param instanceId instance that owns the slice
     */
    default void captureSliceDownloadTimeout(int instanceId)
    {

    }

    /**
     * Captures slice S3 download retry
     *
     * @param instanceId the Cassandra instance ID where the download was retried
     */
    default void captureSliceDownloadRetry(int instanceId)
    {

    }

    /**
     * Capture slice s3 download checksum mismatch
     *
     * @param instanceId instance that owns the slice
     */
    default void captureSliceChecksumMismatch(int instanceId)
    {

    }


    /**
     * Captures the time taken to replicate a slice between s3 regions
     *
     * @param durationNanos duration in nanoseconds
     */
    default void captureSliceReplicationTime(long durationNanos)
    {

    }

    /**
     * Captures the length of the slice import queue
     *
     * @param instanceId instance that contains the SSTables
     * @param length     queue length
     */
    default void captureSliceImportQueueLength(int instanceId, long length)
    {

    }

    /**
     * Captures the count of the pending slices that are submitted, but not started
     *
     * @param instanceId instance that contains the SSTables
     * @param count      the total number of pending slices
     */
    default void capturePendingSliceCount(int instanceId, long count)
    {

    }

    /**
     * Size of the data component of a SSTable
     *
     * @param instanceId  instance that contains the SSTables
     * @param sizeInBytes size in bytes
     */
    default void captureSSTableDataComponentSize(int instanceId, long sizeInBytes)
    {

    }

    /**
     * Overall time it takes to complete a restore job successfully
     *
     * @param durationNanos duration in nanoseconds
     */
    default void captureJobCompletionTime(long durationNanos)
    {

    }

    /**
     * Captures a succeeded job
     */
    default void captureSuccessJob()
    {

    }

    /**
     * Captures a failed job
     */
    default void captureFailedJob()
    {

    }

    /**
     * Captures the count of active jobs
     *
     * @param count number of active jobs
     */
    default void captureActiveJobs(long count)
    {

    }

    /**
     * Captures s3 token is refreshed
     */
    default void captureTokenRefreshed()
    {

    }

    /**
     * Captures s3 token is unauthorized
     */
    default void captureTokenUnauthorized()
    {

    }

    /**
     * Captures s3 token is expired
     */
    default void captureTokenExpired()
    {

    }
}
