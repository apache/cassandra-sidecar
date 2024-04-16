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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.Row;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.data.RestoreSliceStatus;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.exceptions.RestoreJobExceptions;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.metrics.RestoreMetrics;
import org.apache.cassandra.sidecar.restore.RestoreJobUtil;
import org.apache.cassandra.sidecar.restore.RestoreSliceHandler;
import org.apache.cassandra.sidecar.restore.RestoreSliceTask;
import org.apache.cassandra.sidecar.restore.RestoreSliceTracker;
import org.apache.cassandra.sidecar.restore.StorageClient;
import org.apache.cassandra.sidecar.restore.StorageClientPool;
import org.apache.cassandra.sidecar.utils.SSTableImporter;
import org.jetbrains.annotations.NotNull;

/**
 * <p>Data object that contains all values that matter to the restore job slice.</p>
 *
 * <p>How the staged files are organized on disk? For each slice,</p>
 * <ol>
 * <li>the S3 object is downloaded to the path at "stageDirectory/key". It is a zip file.</li>
 * <li>the zip is then extracted to the directory at "stageDirectory/keyspace/table/".
 *    The extracted sstables are imported into Cassandra.</li>
 * </ol>
 */
public class RestoreSlice
{
    private final UUID jobId;
    private final String keyspace;
    private final String table;
    private final String sliceId;
    private final short bucketId;
    private final String bucket;
    private final String key;
    private final String checksum; // etag
    // The path to the directory that stores the s3 object of the slice and the sstables after unzipping.
    // Its value is "baseStageDirectory/uploadId"
    private final Path stageDirectory;
    // The path to the staged s3 object (file). The path is inside stageDirectory.
    // Its value is "stageDirectory/key"
    private final Path stagedObjectPath;
    private final String uploadId;
    private final InstanceMetadata owner;
    private final BigInteger startToken;
    private final BigInteger endToken;
    private final Map<String, RestoreSliceStatus> statusByReplica;
    private final Set<String> replicas;
    private final long creationTimeNanos;
    private final long compressedSize;
    private final long uncompressedSize;
    private RestoreSliceTracker tracker;

    // mutable states
    private boolean existsOnS3 = false;
    private boolean hasStaged = false;
    private boolean hasImported = false;
    private int downloadAttempt = 0;
    private volatile boolean isCancelled = false;

    public static Builder builder()
    {
        return new Builder();
    }

    private RestoreSlice(Builder builder)
    {
        this.jobId = builder.jobId;
        this.keyspace = builder.keyspace;
        this.table = builder.table;
        this.sliceId = builder.sliceId;
        this.bucketId = builder.bucketId;
        this.bucket = builder.bucket;
        this.key = builder.key;
        this.checksum = builder.checksum;
        this.stageDirectory = builder.stageDirectory;
        this.stagedObjectPath = builder.stagedObjectPath;
        this.uploadId = builder.uploadId;
        this.owner = builder.owner;
        this.startToken = builder.startToken;
        this.endToken = builder.endToken;
        this.statusByReplica = builder.statusByReplica;
        this.replicas = builder.replicas;
        this.compressedSize = builder.compressedSize;
        this.uncompressedSize = builder.uncompressedSize;
        this.creationTimeNanos = System.nanoTime();
    }

    public Builder unbuild()
    {
        return new Builder(this);
    }

    @Override
    public int hashCode()
    {
        // Note: destinationPathInStaging and owner are not included as they are 'transient'.
        // status_by_replicas and replicas are not added as instances can be added
        return Objects.hash(jobId, keyspace, table, sliceId, bucketId, bucket, key,
                            checksum, startToken, endToken, compressedSize, uncompressedSize);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof RestoreSlice))
            return false;

        RestoreSlice that = (RestoreSlice) obj;
        // Note: destinationPathInStaging and owner are not included as they are 'transient'.
        // status_by_replicas and replicas are not added as instances can be added
        return Objects.equals(this.jobId, that.jobId)
               && Objects.equals(this.keyspace, that.keyspace)
               && Objects.equals(this.table, that.table)
               && Objects.equals(this.sliceId, that.sliceId)
               && Objects.equals(this.bucketId, that.bucketId)
               && Objects.equals(this.bucket, that.bucket)
               && Objects.equals(this.key, that.key)
               && Objects.equals(this.checksum, that.checksum)
               && Objects.equals(this.startToken, that.startToken)
               && Objects.equals(this.endToken, that.endToken)
               && this.compressedSize == that.compressedSize
               && this.uncompressedSize == that.uncompressedSize;
    }

    // -- INTERNAL FLOW CONTROL METHODS --

    /**
     * Register the {@link RestoreSliceTracker} for the slice
     */
    public void registerTracker(RestoreSliceTracker tracker)
    {
        this.tracker = tracker;
    }

    /**
     * Mark the slice as completed
     */
    public void complete()
    {
        tracker.completeSlice(this);
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
        statusByReplica.put(String.valueOf(instanceId), RestoreSliceStatus.FAILED);
    }

    /**
     * Fail the job, including all its owning slices, with the provided {@link RestoreJobFatalException}.
     */
    public void fail(RestoreJobFatalException exception)
    {
        tracker.fail(exception);
        failAtInstance(owner().id());
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
     * @return {@link RestoreSliceTask} of the restore slice. See {@link RestoreSliceTask} for the steps.
     */
    public RestoreSliceHandler toAsyncTask(StorageClientPool s3ClientPool,
                                           TaskExecutorPool executorPool,
                                           SSTableImporter importer,
                                           double requiredUsableSpacePercentage,
                                           RestoreSliceDatabaseAccessor sliceDatabaseAccessor,
                                           RestoreJobUtil restoreJobUtil)
    {
        if (isCancelled)
            return RestoreSliceTask.failed(RestoreJobExceptions.ofFatalSlice("Restore slice is cancelled",
                                                                                 this, null), this);

        try
        {
            StorageClient s3Client = s3ClientPool.storageClient(job());
            return new RestoreSliceTask(this, s3Client,
                                        executorPool, importer,
                                        requiredUsableSpacePercentage,
                                        sliceDatabaseAccessor,
                                        restoreJobUtil,
                                        owner);
        }
        catch (IllegalStateException illegalState)
        {
            // The slice is not registered with a tracker, retry later.
            return RestoreSliceTask.failed(RestoreJobExceptions.ofSlice("Restore slice is not started",
                                                                           this, illegalState), this);
        }
        catch (Exception cause)
        {
            return RestoreSliceTask.failed(RestoreJobExceptions.ofFatalSlice("Restore slice is failed",
                                                                                this, cause), this);
        }
    }

    // -- (self-explanatory) GETTERS --

    @NotNull
    public final RestoreJob job() // disable override to always lookup from registered tracker
    {
        if (tracker == null)
        {
            throw new IllegalStateException("Restore slice is not registered with a tracker");
        }

        return tracker.restoreJob();
    }

    public UUID jobId()
    {
        return jobId;
    }

    public String keyspace()
    {
        return keyspace;
    }

    public String table()
    {
        return table;
    }

    public String sliceId()
    {
        return sliceId;
    }

    public Short bucketId()
    {
        return this.bucketId;
    }

    public String bucket()
    {
        return bucket;
    }

    public String key()
    {
        return key;
    }

    public String checksum()
    {
        return checksum;
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

    public Map<String, RestoreSliceStatus> statusByReplica()
    {
        return statusByReplica;
    }

    public Set<String> replicas()
    {
        return this.replicas;
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

    public long compressedSize()
    {
        return compressedSize;
    }

    public long uncompressedSize()
    {
        return uncompressedSize;
    }

    public long creationTimeNanos()
    {
        return creationTimeNanos;
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

    // -------------

    public String shortDescription()
    {
        return "SliceId: " + sliceId + ", Key: " + key + ", Bucket: " + bucket + ", Checksum: " + checksum;
    }

    public static RestoreSlice from(Row row)
    {
        Builder builder = new Builder();
        builder.jobId(row.getUUID("job_id"));
        builder.sliceId(row.getString("slice_id"));
        builder.bucketId(row.getShort("bucket_id"));
        builder.storageBucket(row.getString("bucket"));
        builder.storageKey(row.getString("key"));
        builder.checksum(row.getString("checksum"));
        builder.startToken(row.getVarint("start_token"));
        builder.endToken(row.getVarint("end_token"));
        builder.compressedSize(row.getLong("compressed_size"));
        builder.uncompressedSize(row.getLong("uncompressed_size"));
        builder.replicaStatus(row.getMap("status_by_replica", String.class, RestoreSliceStatus.class));
        builder.replicas(row.getSet("all_replicas", String.class));
        return builder.build();
    }

    /**
     * Builder for building a {@link RestoreSlice}
     */
    public static class Builder implements DataObjectBuilder<Builder, RestoreSlice>
    {
        private UUID jobId;
        private String keyspace;
        private String table;
        private String sliceId;
        private short bucketId;
        private String bucket;
        private String key;
        private String checksum; // etag
        private Path stageDirectory;
        private Path stagedObjectPath;
        private String uploadId;
        private InstanceMetadata owner;
        private BigInteger startToken;
        private BigInteger endToken;
        private Map<String, RestoreSliceStatus> statusByReplica;
        private Set<String> replicas;
        private long compressedSize;
        private long uncompressedSize;

        private Builder()
        {
        }

        private Builder(RestoreSlice slice)
        {
            this.jobId = slice.jobId;
            this.keyspace = slice.keyspace;
            this.table = slice.table;
            this.sliceId = slice.sliceId;
            this.bucketId = slice.bucketId;
            this.bucket = slice.bucket;
            this.key = slice.key;
            this.checksum = slice.checksum;
            this.stageDirectory = slice.stageDirectory;
            this.uploadId = slice.uploadId;
            this.owner = slice.owner;
            this.startToken = slice.startToken;
            this.endToken = slice.endToken;
            this.statusByReplica = Collections.unmodifiableMap(slice.statusByReplica);
            this.replicas = Collections.unmodifiableSet(slice.replicas);
        }

        public Builder jobId(UUID jobId)
        {
            return update(b -> b.jobId = jobId);
        }

        public Builder keyspace(String keyspace)
        {
            return update(b -> b.keyspace = keyspace);
        }

        public Builder table(String table)
        {
            return update(b -> b.table = table);
        }

        public Builder sliceId(String sliceId)
        {
            return update(b -> b.sliceId = sliceId);
        }

        public Builder bucketId(short bucketId)
        {
            return update(b -> b.bucketId = bucketId);
        }

        public Builder storageBucket(String storageBucket)
        {
            return update(b -> b.bucket = storageBucket);
        }

        public Builder storageKey(String storageKey)
        {
            return update(b -> b.key = storageKey);
        }

        public Builder checksum(String checksum)
        {
            return update(b -> b.checksum = checksum);
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

        public Builder compressedSize(long compressedSize)
        {
            return update(b -> b.compressedSize = compressedSize);
        }

        public Builder uncompressedSize(long uncompressedSize)
        {
            return update(b -> b.uncompressedSize = uncompressedSize);
        }

        public Builder replicaStatus(Map<String, RestoreSliceStatus> statusByReplica)
        {
            return update(b -> b.statusByReplica = new HashMap<>(statusByReplica));
        }

        public Builder replicas(Set<String> replicas)
        {
            return update(b -> b.replicas = new HashSet<>(replicas));
        }

        /**
         * Bulk set fields with the supplied object {@link QualifiedTableName}
         */
        public Builder qualifiedTableName(QualifiedTableName qualifiedTableName)
        {
            return update(b -> {
                b.keyspace = qualifiedTableName.keyspace();
                b.table = qualifiedTableName.tableName();
            });
        }

        /**
         * Bulk set fields with the supplied object {@link CreateSliceRequestPayload}
         */
        public Builder createSliceRequestPayload(CreateSliceRequestPayload payload)
        {
            return update(b -> {
                b.sliceId = payload.sliceId();
                b.bucketId = payload.bucketIdAsShort();
                b.bucket = payload.bucket();
                b.key = payload.key();
                b.checksum = payload.checksum();
                b.startToken = payload.startToken();
                b.endToken = payload.endToken();
                b.compressedSize = payload.compressedSizeOrZero();
                b.uncompressedSize = payload.uncompressedSizeOrZero();
            });
        }

        @Override
        public RestoreSlice build()
        {
            // precompute the path to the to-be-staged object on disk
            stagedObjectPath = stageDirectory.resolve(key);
            return new RestoreSlice(this);
        }

        @Override
        public Builder self()
        {
            return this;
        }
    }
}

