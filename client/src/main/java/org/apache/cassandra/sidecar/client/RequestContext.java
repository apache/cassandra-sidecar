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

package org.apache.cassandra.sidecar.client;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.sidecar.client.retry.ExponentialBackoffRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.NoRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.RetryPolicy;
import org.apache.cassandra.sidecar.client.selection.InstanceSelectionPolicy;
import org.apache.cassandra.sidecar.client.selection.SingleInstanceSelectionPolicy;
import org.apache.cassandra.sidecar.common.request.CassandraJmxHealthRequest;
import org.apache.cassandra.sidecar.common.request.CassandraNativeHealthRequest;
import org.apache.cassandra.sidecar.common.request.CleanSSTableUploadSessionRequest;
import org.apache.cassandra.sidecar.common.request.ClearSnapshotRequest;
import org.apache.cassandra.sidecar.common.request.CreateSnapshotRequest;
import org.apache.cassandra.sidecar.common.request.GossipInfoRequest;
import org.apache.cassandra.sidecar.common.request.ImportSSTableRequest;
import org.apache.cassandra.sidecar.common.request.ListSnapshotFilesRequest;
import org.apache.cassandra.sidecar.common.request.NodeSettingsRequest;
import org.apache.cassandra.sidecar.common.request.Request;
import org.apache.cassandra.sidecar.common.request.RingRequest;
import org.apache.cassandra.sidecar.common.request.SSTableComponentRequest;
import org.apache.cassandra.sidecar.common.request.SchemaRequest;
import org.apache.cassandra.sidecar.common.request.SidecarHealthRequest;
import org.apache.cassandra.sidecar.common.request.TimeSkewRequest;
import org.apache.cassandra.sidecar.common.request.TokenRangeReplicasRequest;
import org.apache.cassandra.sidecar.common.request.UploadSSTableRequest;
import org.apache.cassandra.sidecar.common.request.data.Digest;
import org.apache.cassandra.sidecar.common.response.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.common.utils.HttpRange;
import org.jetbrains.annotations.Nullable;

/**
 * The context for a given request that include the {@link InstanceSelectionPolicy}, the {@link RetryPolicy}, and
 * the {@link Request}
 */
public class RequestContext
{
    protected static final SidecarHealthRequest SIDECAR_HEALTH_REQUEST = new SidecarHealthRequest();

    /**
     * @deprecated in favor of {@link #CASSANDRA_NATIVE_HEALTH_REQUEST}
     */
    @Deprecated
    protected static final CassandraNativeHealthRequest CASSANDRA_HEALTH_REQUEST =
    new CassandraNativeHealthRequest(true /* useDeprecatedHealthEndpoint */);
    protected static final CassandraNativeHealthRequest CASSANDRA_NATIVE_HEALTH_REQUEST =
    new CassandraNativeHealthRequest();
    protected static final CassandraJmxHealthRequest CASSANDRA_JMX_HEALTH_REQUEST = new CassandraJmxHealthRequest();
    protected static final SchemaRequest FULL_SCHEMA_REQUEST = new SchemaRequest();
    protected static final TimeSkewRequest TIME_SKEW_REQUEST = new TimeSkewRequest();
    protected static final NodeSettingsRequest NODE_SETTINGS_REQUEST = new NodeSettingsRequest();
    protected static final RingRequest RING_REQUEST = new RingRequest();
    protected static final GossipInfoRequest GOSSIP_INFO_REQUEST = new GossipInfoRequest();
    protected static final RetryPolicy DEFAULT_RETRY_POLICY = new NoRetryPolicy();
    protected static final RetryPolicy DEFAULT_EXPONENTIAL_BACKOFF_RETRY_POLICY =
    new ExponentialBackoffRetryPolicy(10, 500L, 60_000L);

    private final InstanceSelectionPolicy instanceSelectionPolicy;
    private final Request request;
    private final RetryPolicy retryPolicy;
    private final Map<String, String> customHeaders;

    private RequestContext(Builder builder)
    {
        instanceSelectionPolicy = builder.instanceSelectionPolicy;
        request = builder.request;
        retryPolicy = builder.retryPolicy;
        customHeaders = Collections.unmodifiableMap(builder.customHeaders);
    }

    /**
     * @return the instance selection policy for the request
     */
    public InstanceSelectionPolicy instanceSelectionPolicy()
    {
        return instanceSelectionPolicy;
    }

    /**
     * @return the retry policy associated to the request
     */
    public RetryPolicy retryPolicy()
    {
        return retryPolicy;
    }

    /**
     * @return the request for the Sidecar service
     */
    public Request request()
    {
        return request;
    }

    /**
     * @return the custom headers for the request
     */
    public Map<String, String> customHeaders()
    {
        return customHeaders;
    }

    /**
     * {@code RequestContext} builder static inner class.
     */
    public static final class Builder
    {
        private InstanceSelectionPolicy instanceSelectionPolicy;
        private Request request;
        private RetryPolicy retryPolicy = DEFAULT_RETRY_POLICY;
        private final Map<String, String> customHeaders;

        public Builder()
        {
            customHeaders = new HashMap<>();
        }

        private Builder(Builder builder)
        {
            request = builder.request;
            instanceSelectionPolicy = builder.instanceSelectionPolicy;
            retryPolicy = builder.retryPolicy;
            customHeaders = new HashMap<>(builder.customHeaders);
        }

        /**
         * Sets the {@code instanceSelectionPolicy} and returns a reference to this Builder enabling method chaining.
         *
         * @param instanceSelectionPolicy the {@code instanceSelectionPolicy} to set
         * @return a reference to this Builder
         */
        public Builder instanceSelectionPolicy(InstanceSelectionPolicy instanceSelectionPolicy)
        {
            this.instanceSelectionPolicy = instanceSelectionPolicy;
            return this;
        }

        /**
         * Sets the {@code instanceSelectionPolicy} to the {@link SingleInstanceSelectionPolicy} for the provided
         * {@code sidecarInstance} and returns a reference to this Builder enabling method chaining.
         *
         * @param sidecarInstance the Sidecar instance where the request will be performed
         * @return a reference to this Builder
         */
        public Builder singleInstanceSelectionPolicy(SidecarInstance sidecarInstance)
        {
            return instanceSelectionPolicy(new SingleInstanceSelectionPolicy(sidecarInstance));
        }

        /**
         * Sets the {@code request} and returns a reference to this Builder enabling method chaining.
         *
         * @param request the {@code request} to set
         * @return a reference to this Builder
         */
        public Builder request(Request request)
        {
            this.request = request;
            return this;
        }

        /**
         * Sets the {@code retryPolicy} and returns a reference to this Builder enabling method chaining.
         *
         * @param retryPolicy the {@code retryPolicy} to set
         * @return a reference to this Builder
         */
        public Builder retryPolicy(RetryPolicy retryPolicy)
        {
            this.retryPolicy = retryPolicy;
            return this;
        }

        /**
         * Sets the {@code request} to be a {@link SidecarHealthRequest}
         * and returns a reference to this Builder enabling method chaining
         *
         * @return a reference to this Builder
         */
        public Builder sidecarHealthRequest()
        {
            return request(SIDECAR_HEALTH_REQUEST);
        }

        /**
         * Sets the {@code request} to be a {@link CassandraNativeHealthRequest}
         * with the {@code useDeprecatedHealthEndpoint} parameter set to {@code true}
         * and returns a reference to this Builder enabling method chaining.
         *
         * @return a reference to this Builder
         * @deprecated in favor of {@link #cassandraNativeHealthRequest()}
         */
        @Deprecated
        public Builder cassandraHealthRequest()
        {
            return request(CASSANDRA_HEALTH_REQUEST);
        }

        /**
         * Sets the {@code request} to be a {@link CassandraNativeHealthRequest}
         * and returns a reference to this Builder enabling method chaining
         *
         * @return a reference to this Builder
         */
        public Builder cassandraNativeHealthRequest()
        {
            return request(CASSANDRA_NATIVE_HEALTH_REQUEST);
        }

        /**
         * Sets the {@code request} to be a {@link CassandraJmxHealthRequest}
         * and returns a reference to this Builder enabling method chaining
         *
         * @return a reference to this Builder
         */
        public Builder cassandraJmxHealthRequest()
        {
            return request(CASSANDRA_JMX_HEALTH_REQUEST);
        }

        /**
         * Sets the {@code request} to be a {@link SchemaRequest} for the full schema and returns a reference to
         * this Builder enabling method chaining.
         *
         * @return a reference to this Builder
         */
        public Builder schemaRequest()
        {
            return request(FULL_SCHEMA_REQUEST);
        }

        /**
         * Sets the {@code request} to be a {@link SchemaRequest} for the specified {@code keyspace} and returns a
         * reference to this Builder enabling method chaining
         *
         * @param keyspace the keyspace in Cassandra
         * @return a reference to this Builder
         */
        public Builder schemaRequest(String keyspace)
        {
            return request(new SchemaRequest(keyspace));
        }

        /**
         * Sets the {@code request} to be a {@link TimeSkewRequest} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @return a reference to this Builder
         */
        public Builder timeSkewRequest()
        {
            return request(TIME_SKEW_REQUEST);
        }

        /**
         * Sets the {@code request} to be a {@link TokenRangeReplicasRequest} and returns a reference to this Builder
         * enabling method chaining.
         *
         * @param keyspace the keyspace in Cassandra
         * @return a reference to this Builder
         */
        public Builder tokenRangeReplicasRequest(String keyspace)
        {
            return request(new TokenRangeReplicasRequest(keyspace));
        }

        /**
         * Sets the {@code request} to be a {@link NodeSettingsRequest} and returns a reference to this Builder
         * enabling method chaining.
         *
         * @return a reference to this Builder
         */
        public Builder nodeSettingsRequest()
        {
            return request(NODE_SETTINGS_REQUEST);
        }

        /**
         * Sets the {@code request} to be a {@link RingRequest} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @return a reference to this Builder
         */
        public Builder ringRequest()
        {
            return request(RING_REQUEST);
        }

        /**
         * Sets the {@code request} to be a {@link RingRequest} for the given {@code keyspace} and returns a reference
         * to this Builder enabling method chaining.
         *
         * @param keyspace the keyspace in Cassandra
         * @return a reference to this Builder
         */
        public Builder ringRequest(String keyspace)
        {
            return request(new RingRequest(keyspace));
        }

        /**
         * Sets the {@code request} to be a {@link GossipInfoRequest} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @return a reference to this Builder
         */
        public Builder gossipInfoRequest()
        {
            return request(GOSSIP_INFO_REQUEST);
        }

        /**
         * Sets the {@code request} to be a {@link SSTableComponentRequest} for the given {@code keyspace},
         * {@code tableName}, {@code snapshotName}, {@code componentName}, requesting the specified {@code range} and
         * returns a reference to this Builder enabling method chaining.
         *
         * @param keyspace      the keyspace in Cassandra
         * @param tableName     the table name in Cassandra
         * @param snapshotName  the name of the snapshot
         * @param componentName the name of the SSTable component
         * @param range         the HTTP range for the request
         * @return a reference to this Builder
         */
        public Builder ssTableComponentRequest(String keyspace,
                                               String tableName,
                                               String snapshotName,
                                               String componentName,
                                               HttpRange range)
        {
            return request(new SSTableComponentRequest(keyspace, tableName, snapshotName, componentName, range));
        }

        /**
         * Sets the {@code request} to be a {@link SSTableComponentRequest} for the requested {@code fileInfo}
         * requesting the specified {@code range} and  returns a reference to this Builder enabling method chaining.
         *
         * @param fileInfo contains information about the file to stream
         * @param range    the HTTP range for the request
         * @return a reference to this Builder
         */
        public Builder ssTableComponentRequest(ListSnapshotFilesResponse.FileInfo fileInfo, HttpRange range)
        {
            return request(new SSTableComponentRequest(fileInfo, range));
        }

        /**
         * Sets the {@code request} to be a {@link ListSnapshotFilesRequest} for the given {@code keyspace},
         * {@code tableName}, and {@code snapshotName} and returns a reference to this Builder enabling method chaining.
         *
         * @param keyspace     the keyspace in Cassandra
         * @param tableName    the table name in Cassandra
         * @param snapshotName the name of the snapshot
         * @return a reference to this Builder
         */
        public Builder listSnapshotFilesRequest(String keyspace, String tableName, String snapshotName)
        {
            return listSnapshotFilesRequest(keyspace, tableName, snapshotName, false);
        }

        /**
         * Sets the {@code request} to be a {@link CreateSnapshotRequest} for the given {@code keyspace},
         * {@code tableName}, and {@code snapshotName} and returns a reference to this Builder enabling method chaining.
         *
         * @param keyspace     the keyspace in Cassandra
         * @param tableName    the table name in Cassandra
         * @param snapshotName the name of the snapshot
         * @param snapshotTTL  an optional time to live option for the snapshot (available since Cassandra 4.1+)
         *                     The TTL option must specify the units, for example 2d represents a TTL for 2 days;
         *                     1h represents a TTL of 1 hour, etc. Valid units are {@code d}, {@code h}, {@code s},
         *                     {@code ms}, {@code us}, {@code µs}, {@code ns}, and {@code m}.
         * @return a reference to this Builder
         */
        public Builder createSnapshotRequest(String keyspace, String tableName, String snapshotName,
                                             @Nullable String snapshotTTL)
        {
            return request(new CreateSnapshotRequest(keyspace, tableName, snapshotName, snapshotTTL));
        }

        /**
         * Sets the {@code request} to be a {@link ListSnapshotFilesRequest} for the given {@code keyspace},
         * {@code tableName}, and {@code snapshotName} and returns a reference to this Builder enabling method chaining.
         *
         * @param keyspace                   the keyspace in Cassandra
         * @param tableName                  the table name in Cassandra
         * @param snapshotName               the name of the snapshot
         * @param includeSecondaryIndexFiles whether to include secondary index files
         * @return a reference to this Builder
         */
        public Builder listSnapshotFilesRequest(String keyspace, String tableName, String snapshotName,
                                                boolean includeSecondaryIndexFiles)
        {
            return request(new ListSnapshotFilesRequest(keyspace, tableName, snapshotName, includeSecondaryIndexFiles));
        }

        /**
         * Sets the {@code request} to be a {@link ClearSnapshotRequest} for the given {@code keyspace},
         * {@code tableName}, and {@code snapshotName} and returns a reference to this Builder enabling method chaining.
         *
         * @param keyspace     the keyspace in Cassandra
         * @param tableName    the table name in Cassandra
         * @param snapshotName the name of the snapshot
         * @return a reference to this Builder
         */
        public Builder clearSnapshotRequest(String keyspace, String tableName, String snapshotName)
        {
            return request(new ClearSnapshotRequest(keyspace, tableName, snapshotName));
        }

        /**
         * Sets the {@code request} to be a {@link CleanSSTableUploadSessionRequest} for the given {@code uploadId}
         * and returns a reference to this Builder enabling method chaining.
         *
         * @param uploadId the unique identifier for the upload
         * @return a reference to this Builder
         */
        public Builder cleanSSTableUploadSessionRequest(String uploadId)
        {
            return request(new CleanSSTableUploadSessionRequest(uploadId));
        }

        /**
         * Sets the {@code request} to be a {@link ImportSSTableRequest} for the given {@code keyspace},
         * {@code tableName}, {@code uploadId}, and selected {@link ImportSSTableRequest.ImportOptions importOptions}
         * and returns a reference to this Builder enabling method chaining.
         *
         * @param keyspace      the keyspace in Cassandra
         * @param tableName     the table name in Cassandra
         * @param uploadId      an identifier for the upload
         * @param importOptions additional options for the import process
         * @return a reference to this Builder
         */
        public Builder importSSTableRequest(String keyspace, String tableName, String uploadId,
                                            ImportSSTableRequest.ImportOptions importOptions)
        {
            return request(new ImportSSTableRequest(keyspace, tableName, uploadId, importOptions));
        }

        /**
         * Sets the {@code} request to be a {@link UploadSSTableRequest} for the given {@code keyspace},
         * {@code tableName}, {@code uploadId}, {@code component}, and returns a
         * reference to this Builder enabling method chaining.
         *
         * @param keyspace  the keyspace in Cassandra
         * @param tableName the table name in Cassandra
         * @param uploadId  an identifier for the upload
         * @param component SSTable component being uploaded
         * @param digest    digest value to check integrity of SSTable component uploaded
         * @param filename  the path to the file to be uploaded
         * @return a reference to this Builder
         */
        public Builder uploadSSTableRequest(String keyspace, String tableName, String uploadId, String component,
                                            Digest digest, String filename)
        {
            return request(new UploadSSTableRequest(keyspace, tableName, uploadId, component, digest, filename));
        }

        /**
         * Sets the {@code retryPolicy} to be an
         * {@link org.apache.cassandra.sidecar.client.retry.ExponentialBackoffRetryPolicy} configured with
         * {@code 10} {@code maxRetries}, {@code 500} {@code retryDelayMillis}, and {@code 60,000}
         * {@code maxRetryDelayMillis}; and returns a reference to this Builder enabling method chaining.
         *
         * @return a reference to this Builder
         */
        public Builder exponentialBackoffRetryPolicy()
        {
            return retryPolicy(DEFAULT_EXPONENTIAL_BACKOFF_RETRY_POLICY);
        }

        /**
         * Sets the {@code retryPolicy} to be an
         * {@link org.apache.cassandra.sidecar.client.retry.ExponentialBackoffRetryPolicy} configured with the provided
         * {@code maxRetries}, {@code retryDelayMillis}, and {@code maxRetryDelayMillis}; and returns a reference to
         * this Builder enabling method chaining.
         *
         * @param maxRetries          the maximum number of retries
         * @param retryDelayMillis    the delay between retries in milliseconds
         * @param maxRetryDelayMillis the maximum retry delay in milliseconds
         * @return a reference to this Builder
         */
        public Builder exponentialBackoffRetryPolicy(int maxRetries, long retryDelayMillis, long maxRetryDelayMillis)
        {
            return retryPolicy(new ExponentialBackoffRetryPolicy(maxRetries, retryDelayMillis, maxRetryDelayMillis));
        }

        /**
         * Adds a custom header for the request and returns a reference to this Builder enabling method chaining.
         *
         * @param name  the header name
         * @param value the header value
         * @return a reference to this Builder
         */
        public Builder addCustomHeader(String name, String value)
        {
            customHeaders.put(name, value);
            return this;
        }

        /**
         * @return a new Builder with references to the parameters previously set in this Builder
         */
        public Builder copy()
        {
            return new Builder(this);
        }

        /**
         * Returns a {@code RequestContext} built from the parameters previously set.
         *
         * @return a {@code RequestContext} built with parameters of this {@code RequestContext.Builder}
         */
        public RequestContext build()
        {
            return new RequestContext(this);
        }
    }
}
