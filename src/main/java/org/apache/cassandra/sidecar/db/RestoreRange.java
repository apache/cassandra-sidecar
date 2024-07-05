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

package org.apache.cassandra.sidecar.db;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

import com.datastax.driver.core.Row;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.locator.LocalTokenRangesProvider;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.response.data.RestoreRangeJson;
import org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.exceptions.RestoreJobExceptions;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.restore.RestoreJobProgressTracker;
import org.apache.cassandra.sidecar.restore.RestoreJobUtil;
import org.apache.cassandra.sidecar.restore.RestoreRangeHandler;
import org.apache.cassandra.sidecar.restore.RestoreRangeTask;
import org.apache.cassandra.sidecar.restore.StorageClient;
import org.apache.cassandra.sidecar.restore.StorageClientPool;
import org.apache.cassandra.sidecar.utils.SSTableImporter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * A {@link RestoreRange}, similar to {@link RestoreSlice}, represents the data of a narrower token range.
 *
 * Conceptually, a {@link RestoreSlice} can be split into multiple {@link RestoreRange}s. A range only belongs to,
 * i.e. fully enclosed in, a slice. In other words, a range is derived from a lice.
 * As slices do not overlap, the ranges have no overlap too.
 * When no split is needed for a slice, its range is equivalent to itself, in terms of token range.
 *
 * In additional, {@link RestoreRange} contains the control flow of applying/importing data to Cassandra.
 *
 * Why is {@link RestoreRange} required?
 * Range is introduced to better align with the current Cassandra token topology.
 * Restore slice represents the client-side generated dataset and its token range,
 * submitted via the create slice API.
 * On the server side, especially the token topology of Cassandra has changed, there can be no exact match of the
 * token range of a slice and the Cassandra node's owning token range. The slice has to be split into ranges that fit
 * into the Cassandra nodes properly.
 *
 *
 * How the staged files are organized on disk?
 * For each slice,
 * - the S3 object is downloaded to the path at "stageDirectory/key". It is a zip file.
 * - the zip is then extracted to the directory at "stageDirectory/keyspace/table/".
 *   The extracted sstables are imported into Cassandra.
 */
public class RestoreRange
{
    // @NotNull fields are persisted
    @NotNull
    private final UUID jobId;
    @NotNull
    private final short bucketId;
    @NotNull
    private final String sliceId;
    @NotNull
    private final String sliceBucket;
    @NotNull
    private final String sliceKey;
    @NotNull
    private final BigInteger startToken;
    @NotNull
    private final BigInteger endToken;
    @NotNull
    private final Map<String, RestoreRangeStatus> statusByReplica;

    @Nullable
    private final RestoreSlice source;

    // The path to the directory that stores the s3 object of the slice and the sstables after unzipping.
    // Its value is "baseStageDirectory/uploadId"
    private final Path stageDirectory;
    // The path to the staged s3 object (file). The path is inside stageDirectory.
    // Its value is "stageDirectory/key"
    private final Path stagedObjectPath;
    private final String uploadId;
    private final InstanceMetadata owner;
    private final RestoreJobProgressTracker tracker;

    // mutable states
    private boolean existsOnS3 = false;
    private boolean hasStaged = false;
    private boolean hasImported = false;
    private int downloadAttempt = 0;
    private volatile boolean isCancelled = false;

    public static RestoreRange from(Row row)
    {
        return new Builder()
               .jobId(row.getUUID("job_id"))
               .bucketId(row.getShort("bucket_id"))
               .startToken(row.getVarint("start_token"))
               .endToken(row.getVarint("end_token"))
               .replicaStatusText(row.getMap("status_by_replica", String.class, String.class))
               .sliceId(row.getString("slice_id"))
               .sliceBucket(row.getString("slice_bucket"))
               .sliceKey(row.getString("slice_key"))
               .build();
    }

    public static Builder builderFromSlice(RestoreSlice slice)
    {
        return new Builder().sourceSlice(slice);
    }

    private RestoreRange(Builder builder)
    {
        this.jobId = builder.jobId;
        this.bucketId = builder.bucketId;
        this.startToken = builder.startToken;
        this.endToken = builder.endToken;
        this.source = builder.sourceSlice;
        this.sliceId = builder.sliceId;
        this.sliceKey = builder.sliceKey;
        this.sliceBucket = builder.sliceBucket;
        this.stageDirectory = builder.stageDirectory;
        this.stagedObjectPath = builder.stagedObjectPath;
        this.uploadId = builder.uploadId;
        this.owner = builder.owner;
        this.statusByReplica = builder.statusByReplica;
        this.tracker = builder.tracker;
    }

    public Builder unbuild()
    {
        return new Builder(this);
    }

    public RestoreRangeJson toJson()
    {
        return new RestoreRangeJson(sliceId, bucketId, sliceBucket, sliceKey, startToken, endToken);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(startToken, endToken, jobId, bucketId, sliceId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof RestoreRange))
            return false;

        RestoreRange that = (RestoreRange) obj;
        // Note: destinationPathInStaging and owner are not included as they are 'transient'.
        // Mutable states are not included, e.g. status_by_replicas.
        return Objects.equals(this.startToken, that.startToken)
               && Objects.equals(this.endToken, that.endToken)
               && Objects.equals(this.jobId, that.jobId)
               && Objects.equals(this.bucketId, that.bucketId)
               && Objects.equals(this.sliceId, that.sliceId);
    }

    // -- INTERNAL FLOW CONTROL METHODS --

    /**
     * Mark the slice as completed
     */
    public void complete()
    {
        tracker.completeRange(this);
    }

    /**
     * Mark the slice has completed the stage phase
     */
    public void completeStagePhase()
    {
        this.hasStaged = true;
    }

    /**
     * Mark the slice has completed the import phase
     */
    public void completeImportPhase()
    {
        this.hasImported = true;
    }

    public void failAtInstance(int instanceId)
    {
        statusByReplica.put(String.valueOf(instanceId), RestoreRangeStatus.FAILED);
    }

    /**
     * Fail the job, including all its owning slices, with the provided {@link RestoreJobFatalException}.
     */
    public void fail(RestoreJobFatalException exception)
    {
        tracker.fail(exception);
        failAtInstance(owner().id());
    }

    /**
     * Request to clean up out of range data. It is requested when detecting the slice contains out of range data
     */
    public void requestOutOfRangeDataCleanup()
    {
        tracker.requestOutOfRangeDataCleanup();
    }

    public void setExistsOnS3()
    {
        this.existsOnS3 = true;
    }

    public void incrementDownloadAttempt()
    {
        this.downloadAttempt++;
    }

    /**
     * Cancel the slice to prevent processing them in the future.
     */
    public void cancel()
    {
        isCancelled = true;
    }

    /**
     * @return {@link RestoreRangeTask} that defines the steps to download and import data into Cassandra
     */
    public RestoreRangeHandler toAsyncTask(StorageClientPool s3ClientPool,
                                           TaskExecutorPool executorPool,
                                           SSTableImporter importer,
                                           double requiredUsableSpacePercentage,
                                           RestoreRangeDatabaseAccessor rangeDatabaseAccessor,
                                           RestoreJobUtil restoreJobUtil,
                                           LocalTokenRangesProvider localTokenRangesProvider,
                                           SidecarMetrics metrics)
    {
        // All submitted range should be registered with a tracker and a source slice.
        // Otherwise, it is an unexpected state and cannot be retried
        if (!canProduceTask())
        {
            return RestoreRangeTask.failed(RestoreJobExceptions.ofFatal("Restore range is missing progress tracker or source slice",
                                                                        this, null), this);
        }

        if (isCancelled)
        {
            return RestoreRangeTask.failed(RestoreJobExceptions.ofFatal("Restore range is cancelled",
                                                                        this, null), this);
        }

        try
        {
            StorageClient s3Client = s3ClientPool.storageClient(job());
            return new RestoreRangeTask(this, s3Client,
                                        executorPool, importer,
                                        requiredUsableSpacePercentage,
                                        rangeDatabaseAccessor,
                                        restoreJobUtil,
                                        localTokenRangesProvider,
                                        metrics);
        }
        catch (Exception cause)
        {
            return RestoreRangeTask.failed(RestoreJobExceptions.ofFatal("Restore range is failed",
                                                                        this, cause), this);
        }
    }

    // -- (self-explanatory) GETTERS --

    @NotNull // use `final` to disable override to ensure always lookup from registered tracker
    public final RestoreJob job()
    {
        return tracker.restoreJob();
    }

    public UUID jobId()
    {
        return jobId;
    }

    public String sliceId()
    {
        return sliceId;
    }

    public String sliceKey()
    {
        return sliceKey;
    }

    public String sliceBucket()
    {
        return sliceBucket;
    }

    public String sliceChecksum()
    {
        return readSliceProperty(RestoreSlice::checksum);
    }

    public Long sliceCreationTimeNanos()
    {
        return readSliceProperty(RestoreSlice::creationTimeNanos);
    }

    public Long sliceCompressedSize()
    {
        return readSliceProperty(RestoreSlice::compressedSize);
    }

    public Long sliceUncompressedSize()
    {
        return readSliceProperty(RestoreSlice::uncompressedSize);
    }

    public String keyspace()
    {
        return readSliceProperty(RestoreSlice::keyspace);
    }

    public String table()
    {
        return readSliceProperty(RestoreSlice::table);
    }

    public short bucketId()
    {
        return bucketId;
    }

    public String uploadId()
    {
        return uploadId;
    }

    public BigInteger startToken()
    {
        return this.startToken;
    }

    public BigInteger endToken()
    {
        return this.endToken;
    }

    public Map<String, RestoreRangeStatus> statusByReplica()
    {
        return statusByReplica;
    }

    public Map<String, String> statusTextByReplica()
    {
        Map<String, String> result = new HashMap<>(statusByReplica.size());
        statusByReplica.forEach((k, v) -> result.put(k, v.name()));
        return result;
    }

    /**
     * @return the path to the directory that stores the s3 object of the slice
     *         and the sstables after unzipping
     */
    public Path stageDirectory()
    {
        return stageDirectory;
    }

    /**
     * @return the path to the staged s3 object
     */
    public Path stagedObjectPath()
    {
        return stagedObjectPath;
    }

    public InstanceMetadata owner()
    {
        return owner;
    }

    public boolean existsOnS3()
    {
        return existsOnS3;
    }

    public boolean hasStaged()
    {
        return hasStaged;
    }

    public boolean hasImported()
    {
        return hasImported;
    }

    public int downloadAttempt()
    {
        return downloadAttempt;
    }

    public boolean isCancelled()
    {
        return isCancelled;
    }

    /**
     * A {@link RestoreRange} is eligible to produce {@link RestoreRangeTask} only if it is backed by both the source {@link RestoreSlice}
     * and the {@link RestoreJobProgressTracker}
     * Otherwise, the {@link RestoreRange} is loaded from persistence, and it is only good for restore job progress check.
     *
     * @return true if it can produce task; false otherwise
     */
    public boolean canProduceTask()
    {
        return tracker != null && source != null;
    }

    public long estimatedSpaceRequiredInBytes()
    {
        Preconditions.checkState(source != null, "Cannot estimate space requirement without source slice");
        return source.compressedSize() + source.uncompressedSize();
    }

    // -------------

    public String shortDescription()
    {
        return "StartToken: " + startToken + ", EndToken: " + endToken +
               ", SliceId: " + sliceId +
               ", Key: " + sliceKey() +
               ", Bucket: " + sliceBucket() +
               ", Checksum: " + sliceChecksum();
    }

    @VisibleForTesting
    public RestoreJobProgressTracker trackerUnsafe()
    {
        return tracker;
    }

    @Nullable
    private <T> T readSliceProperty(Function<RestoreSlice, T> func)
    {
        if (source == null)
        {
            return null;
        }
        return func.apply(source);
    }

    /**
     * Builder for building a {@link RestoreRange}
     */
    public static class Builder implements DataObjectBuilder<Builder, RestoreRange>
    {
        private UUID jobId;
        private short bucketId;
        private BigInteger startToken;
        private BigInteger endToken;
        private RestoreSlice sourceSlice;
        private String sliceId;
        private String sliceKey;
        private String sliceBucket;
        private InstanceMetadata owner;
        private Path stageDirectory;
        private Path stagedObjectPath;
        private String uploadId;
        private Map<String, RestoreRangeStatus> statusByReplica;
        private RestoreJobProgressTracker tracker = null;

        private Builder()
        {
        }

        private Builder(RestoreRange range)
        {
            this.jobId = range.jobId;
            this.bucketId = range.bucketId;
            this.sourceSlice = range.source;
            this.sliceId = range.sliceId;
            this.sliceKey = range.sliceKey;
            this.sliceBucket = range.sliceBucket;
            this.stageDirectory = range.stageDirectory;
            this.uploadId = range.uploadId;
            this.owner = range.owner;
            this.startToken = range.startToken;
            this.endToken = range.endToken;
            this.statusByReplica = Collections.unmodifiableMap(range.statusByReplica);
            this.tracker = range.tracker;
        }

        public Builder jobId(UUID jobId)
        {
            return update(b -> b.jobId = jobId);
        }

        public Builder bucketId(short bucketId)
        {
            return update(b -> b.bucketId = bucketId);
        }

        public Builder sourceSlice(RestoreSlice sourceSlice)
        {
            return update(b -> b.sourceSlice = sourceSlice)
                   .jobId(sourceSlice.jobId())
                   .sliceId(sourceSlice.sliceId())
                   .sliceBucket(sourceSlice.bucket())
                   .sliceKey(sourceSlice.key())
                   .bucketId(sourceSlice.bucketId())
                   .startToken(sourceSlice.startToken())
                   .endToken(sourceSlice.endToken());
        }

        public Builder sliceId(String sliceId)
        {
            return update(b -> b.sliceId = sliceId);
        }

        public Builder sliceBucket(String sliceBucket)
        {
            return update(b -> b.sliceBucket = sliceBucket);
        }

        public Builder sliceKey(String sliceKey)
        {
            return update(b -> b.sliceKey = sliceKey);
        }

        public Builder stageDirectory(Path basePath, String uploadId)
        {
            return update(b -> {
                b.stageDirectory = basePath.resolve(uploadId);
                b.uploadId = uploadId;
            });
        }

        public Builder ownerInstance(InstanceMetadata owner)
        {
            return update(b -> b.owner = owner);
        }

        public Builder startToken(BigInteger startToken)
        {
            return update(b -> b.startToken = startToken);
        }

        public Builder endToken(BigInteger endToken)
        {
            return update(b -> b.endToken = endToken);
        }

        public Builder replicaStatus(Map<String, RestoreRangeStatus> statusByReplica)
        {
            return update(b -> b.statusByReplica = new HashMap<>(statusByReplica));
        }

        public Builder replicaStatusText(Map<String, String> statusTextByReplica)
        {
            Map<String, RestoreRangeStatus> map = new HashMap<>(statusTextByReplica.size());
            statusTextByReplica.forEach((k, v) -> map.put(k, RestoreRangeStatus.valueOf(v)));
            return replicaStatus(map);
        }

        public Builder restoreJobProgressTracker(RestoreJobProgressTracker tracker)
        {
            return update(b -> b.tracker = tracker);
        }

        @Override
        public RestoreRange build()
        {
            if (sourceSlice != null)
            {
                // precompute the path to the to-be-staged object on disk
                stagedObjectPath = stageDirectory.resolve(sourceSlice.key());
            }
            return new RestoreRange(this);
        }

        @Override
        public Builder self()
        {
            return this;
        }

        @VisibleForTesting
        public Builder unsetSourceSlice()
        {
            return update(b -> b.sourceSlice = null);
        }
    }
}
