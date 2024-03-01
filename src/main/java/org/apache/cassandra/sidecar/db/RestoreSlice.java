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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.request.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.common.response.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.server.cluster.locator.Token;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;

/**
 * Data object that contains all values that matter to the restore job slice.
 */
public class RestoreSlice
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreSlice.class);

    private final UUID jobId;
    private final String keyspace;
    private final String table;
    private final String sliceId;
    private final short bucketId;
    private final String bucket;
    private final String key;
    private final String checksum; // etag
    private final BigInteger startToken;
    private final BigInteger endToken;
    private final long creationTimeNanos;
    private final long compressedSize;
    private final long uncompressedSize;

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
        this.startToken = builder.startToken;
        this.endToken = builder.endToken;
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

    /**
     * Splits the slice based on the topology changes.
     * <p></p>
     * When the current topology has pending ranges, it results into the slice S being split into
     * multiple sub-ranges, say, s1 and s2. The updater splits, and persists the new slices to database.
     * The original slice S is then deleted.
     * The new slices still reference to the same s3 object, i.e. {@code <bucket/key/checksum>}
     *
     * @param topology topology to guide the splitting
     * @return a list of slices of the sub-ranges
     */
    public List<RestoreSlice> splitMaybe(@Nullable TokenRangeReplicasResponse topology)
    {
        if (topology == null)
        {
            return Collections.singletonList(this);
        }

        List<RestoreSlice> splits = new ArrayList<>();
        TokenRange sliceRange = new TokenRange(startToken(), endToken());
        for (TokenRangeReplicasResponse.ReplicaInfo replicaInfo : topology.writeReplicas())
        {
            TokenRange range = new TokenRange(Token.from(replicaInfo.start()), Token.from(replicaInfo.end()));
            if (range.encloses(sliceRange))
            {
                return Collections.singletonList(this);
            }
            else if (range.overlaps(sliceRange))
            {
                TokenRange intersection = range.intersection(sliceRange);
                // Adjust the slice range to match with the write replica and persist.
                // The object location remains the same as sidecar need to download the same object.
                // It just needs a narrower range of data within the slice
                splits.add(unbuild()
                           .startToken(intersection.start().toBigInteger())
                           .endToken(intersection.end().toBigInteger())
                           .build());
            }

            if (range.largerThan(sliceRange))
            {
                // all following ranges are larger the original range; exit the iteration early.
                break;
            }
        }

        if (splits.isEmpty())
        {
            throw new IllegalStateException("Token range of the slice is not found in the write replicas. " +
                                            "slice range: " + formatRange(startToken, endToken));
        }
        else
        {
            LOGGER.info("The slice is split. splitsCount={} splits={}",
                        splits.size(), splits.stream().map(s -> formatRange(s.startToken, s.endToken))
                        .collect(Collectors.toList()));
        }

        return splits;
    }

    private String formatRange(BigInteger start, BigInteger end)
    {
        return "(" + start + ", " + end + ']';
    }

    // -- (self-explanatory) GETTERS --

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

    public BigInteger startToken()
    {
        return this.startToken;
    }

    public BigInteger endToken()
    {
        return this.endToken;
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

    // -------------

    public static RestoreSlice from(Row row, RestoreJob restoreJob)
    {
        Builder builder = new Builder();
        builder.jobId(row.getUUID("job_id"));
        builder.keyspace(restoreJob.keyspaceName);
        builder.table(restoreJob.tableName);
        builder.sliceId(row.getString("slice_id"));
        builder.bucketId(row.getShort("bucket_id"));
        builder.storageBucket(row.getString("bucket"));
        builder.storageKey(row.getString("key"));
        builder.checksum(row.getString("checksum"));
        builder.startToken(row.getVarint("start_token"));
        builder.endToken(row.getVarint("end_token"));
        builder.compressedSize(row.getLong("compressed_size"));
        builder.uncompressedSize(row.getLong("uncompressed_size"));
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
        private BigInteger startToken;
        private BigInteger endToken;
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
            this.startToken = slice.startToken;
            this.endToken = slice.endToken;
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
            return new RestoreSlice(this);
        }

        @Override
        public Builder self()
        {
            return this;
        }
    }
}

