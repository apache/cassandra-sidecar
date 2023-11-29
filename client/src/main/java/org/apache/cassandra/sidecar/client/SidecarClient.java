/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.client;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.cassandra.sidecar.client.request.AbortRestoreJobRequest;
import org.apache.cassandra.sidecar.client.request.CreateRestoreJobRequest;
import org.apache.cassandra.sidecar.client.request.CreateRestoreJobSliceRequest;
import org.apache.cassandra.sidecar.client.request.ImportSSTableRequest;
import org.apache.cassandra.sidecar.client.request.RestoreJobSummaryRequest;
import org.apache.cassandra.sidecar.client.request.UpdateRestoreJobRequest;
import org.apache.cassandra.sidecar.client.retry.CreateRestoreJobRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.IgnoreConflictRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.OncePerInstanceRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.RetryPolicy;
import org.apache.cassandra.sidecar.client.retry.RunnableOnStatusCodeRetryPolicy;
import org.apache.cassandra.sidecar.client.selection.InstanceSelectionPolicy;
import org.apache.cassandra.sidecar.client.selection.RandomInstanceSelectionPolicy;
import org.apache.cassandra.sidecar.common.NodeSettings;
import org.apache.cassandra.sidecar.common.data.CreateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.data.CreateRestoreJobResponsePayload;
import org.apache.cassandra.sidecar.common.data.CreateSliceRequestPayload;
import org.apache.cassandra.sidecar.common.data.Digest;
import org.apache.cassandra.sidecar.common.data.GossipInfoResponse;
import org.apache.cassandra.sidecar.common.data.HealthResponse;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.common.data.RestoreJobSummaryResponsePayload;
import org.apache.cassandra.sidecar.common.data.RingResponse;
import org.apache.cassandra.sidecar.common.data.SSTableImportResponse;
import org.apache.cassandra.sidecar.common.data.SchemaResponse;
import org.apache.cassandra.sidecar.common.data.TimeSkewResponse;
import org.apache.cassandra.sidecar.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.sidecar.common.data.UpdateRestoreJobRequestPayload;
import org.apache.cassandra.sidecar.common.utils.HttpRange;
import org.jetbrains.annotations.Nullable;

/**
 * The SidecarClient class to perform requests
 */
public class SidecarClient implements AutoCloseable, SidecarClientBlobRestoreExtension
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarClient.class);

    protected RequestExecutor executor;
    protected final RetryPolicy defaultRetryPolicy;
    protected final RetryPolicy ignoreConflictRetryPolicy;
    protected final RetryPolicy oncePerInstanceRetryPolicy;
    protected RequestContext.Builder baseBuilder;

    public SidecarClient(SidecarInstancesProvider instancesProvider,
                         RequestExecutor requestExecutor,
                         SidecarClientConfig sidecarClientConfig,
                         RetryPolicy defaultRetryPolicy)
    {
        this.defaultRetryPolicy = defaultRetryPolicy;
        ignoreConflictRetryPolicy = new IgnoreConflictRetryPolicy(sidecarClientConfig.maxRetries(),
                                                                  sidecarClientConfig.retryDelayMillis(),
                                                                  sidecarClientConfig.maxRetryDelayMillis());
        oncePerInstanceRetryPolicy = new OncePerInstanceRetryPolicy(sidecarClientConfig.minimumHealthRetryDelay(),
                                                                    sidecarClientConfig.maximumHealthRetryDelay());
        baseBuilder = new RequestContext.Builder()
                      .instanceSelectionPolicy(new RandomInstanceSelectionPolicy(instancesProvider))
                      .retryPolicy(defaultRetryPolicy);
        executor = requestExecutor;
    }

    /**
     * Executes the Sidecar health request using the configured selection policy and with no retries
     *
     * @return a completable future of the Sidecar health response
     */
    public CompletableFuture<HealthResponse> sidecarHealth()
    {
        return executor.executeRequestAsync(requestBuilder()
                                            .sidecarHealthRequest()
                                            .retryPolicy(oncePerInstanceRetryPolicy)
                                            .build());
    }

    /**
     * Executes the Cassandra health request using the configured selection policy and with no retries
     *
     * @return a completable future of the Cassandra health response
     * @deprecated use {@link #cassandraNativeHealth()} instead
     */
    @Deprecated
    public CompletableFuture<HealthResponse> cassandraHealth()
    {
        return executor.executeRequestAsync(requestBuilder()
                                            .cassandraHealthRequest()
                                            .retryPolicy(oncePerInstanceRetryPolicy)
                                            .build());
    }

    /**
     * Executes the Cassandra health request using the configured selection policy and with no retries
     *
     * @return a completable future of the Cassandra health response
     */
    public CompletableFuture<HealthResponse> cassandraNativeHealth()
    {
        return executor.executeRequestAsync(requestBuilder()
                                            .cassandraNativeHealthRequest()
                                            .retryPolicy(new OncePerInstanceRetryPolicy())
                                            .build());
    }

    /**
     * Executes the Cassandra health request using the configured selection policy and with no retries
     *
     * @return a completable future of the Cassandra health response
     */
    public CompletableFuture<HealthResponse> cassandraJmxHealth()
    {
        return executor.executeRequestAsync(requestBuilder()
                                            .cassandraJmxHealthRequest()
                                            .retryPolicy(new OncePerInstanceRetryPolicy())
                                            .build());
    }

    /**
     * Executes the full schema request using the default retry policy and configured selection policy
     *
     * @return a completable future of the full schema response
     */
    public CompletableFuture<SchemaResponse> fullSchema()
    {
        return executor.executeRequestAsync(requestBuilder().schemaRequest().build());
    }

    /**
     * Executes the schema request for the {@code keyspace} using the default retry policy and configured selection
     * policy
     *
     * @param keyspace the keyspace in Cassandra
     * @return a completable future of the schema response for the provided {@code keyspace}
     */
    public CompletableFuture<SchemaResponse> schema(String keyspace)
    {
        return executor.executeRequestAsync(requestBuilder().schemaRequest(keyspace).build());
    }

    /**
     * Executes the ring request for the {@code keyspace} using the default retry policy and configured selection
     * policy
     *
     * @param keyspace the keyspace in Cassandra
     * @return a completable future of the ring response for the provided {@code keyspace}
     */
    public CompletableFuture<RingResponse> ring(String keyspace)
    {
        return executor.executeRequestAsync(requestBuilder().ringRequest(keyspace).build());
    }

    /**
     * Executes the node settings request using the default retry policy and configured selection policy
     *
     * @return a completable future of the node settings
     */
    public CompletableFuture<NodeSettings> nodeSettings()
    {
        return executor.executeRequestAsync(requestBuilder().nodeSettingsRequest().build());
    }

    /**
     * Executes the node settings request using the default retry policy and provided {@code instance}
     *
     * @param instance the instance where the request will be executed
     * @return a completable future of the node settings
     */
    public CompletableFuture<NodeSettings> nodeSettings(SidecarInstance instance)
    {
        return executor.executeRequestAsync(requestBuilder().singleInstanceSelectionPolicy(instance)
                                                            .nodeSettingsRequest()
                                                            .build());
    }

    /**
     * Executes the gossip info request using the default retry policy and configured selection policy
     *
     * @return a completable future of the gossip info
     */
    public CompletableFuture<GossipInfoResponse> gossipInfo()
    {
        return executor.executeRequestAsync(requestBuilder().gossipInfoRequest().build());
    }

    /**
     * Executes the time skew request using the default retry policy and configured selection policy
     *
     * @return a completable future of the time skew
     */
    public CompletableFuture<TimeSkewResponse> timeSkew()
    {
        return executor.executeRequestAsync(requestBuilder().timeSkewRequest().build());
    }

    /**
     * Executes the time skew request using the default retry policy and uses random instance selection policy
     * with the provided instances
     *
     * @param instances the list of Sidecar instances to try for this request
     * @return a completable future of the time skew
     */
    public CompletableFuture<TimeSkewResponse> timeSkew(List<? extends SidecarInstance> instances)
    {
        SidecarInstancesProvider instancesProvider = new SimpleSidecarInstancesProvider(instances);
        InstanceSelectionPolicy instanceSelectionPolicy = new RandomInstanceSelectionPolicy(instancesProvider);
        return executor.executeRequestAsync(requestBuilder()
                                            .instanceSelectionPolicy(instanceSelectionPolicy)
                                            .timeSkewRequest()
                                            .build());
    }

    /**
     * Executes the token-range replicas request using the default retry policy and configured selection policy
     *
     * @param instances the list of Sidecar instances to try for this request
     * @param keyspace  the keyspace in Cassandra
     * @return a completable future of the token-range replicas
     */
    public CompletableFuture<TokenRangeReplicasResponse> tokenRangeReplicas(List<? extends SidecarInstance> instances,
                                                                            String keyspace)
    {
        SidecarInstancesProvider instancesProvider = new SimpleSidecarInstancesProvider(instances);
        InstanceSelectionPolicy instanceSelectionPolicy = new RandomInstanceSelectionPolicy(instancesProvider);
        return executeRequestAsync(requestBuilder()
                                   .instanceSelectionPolicy(instanceSelectionPolicy)
                                   .tokenRangeReplicasRequest(keyspace)
                                   .build());
    }

    /**
     * Executes the list snapshot files request including secondary index files using the default retry policy and
     * provided {@code instance}
     *
     * @param instance     the instance where the request will be executed
     * @param keyspace     the keyspace in Cassandra
     * @param table        the table name in Cassandra
     * @param snapshotName the name of the snapshot
     * @return a completable future for the request
     */
    public CompletableFuture<ListSnapshotFilesResponse> listSnapshotFiles(SidecarInstance instance,
                                                                          String keyspace,
                                                                          String table,
                                                                          String snapshotName)
    {
        return listSnapshotFiles(instance, keyspace, table, snapshotName, true);
    }


    /**
     * Executes the list snapshot files request using the default retry policy and provided {@code instance}
     *
     * @param instance                   the instance where the request will be executed
     * @param keyspace                   the keyspace in Cassandra
     * @param table                      the table name in Cassandra
     * @param snapshotName               the name of the snapshot
     * @param includeSecondaryIndexFiles whether to include secondary index files
     * @return a completable future for the request
     */
    public CompletableFuture<ListSnapshotFilesResponse> listSnapshotFiles(SidecarInstance instance,
                                                                          String keyspace,
                                                                          String table,
                                                                          String snapshotName,
                                                                          boolean includeSecondaryIndexFiles)
    {
        return executor.executeRequestAsync(requestBuilder().singleInstanceSelectionPolicy(instance)
                                                            .listSnapshotFilesRequest(keyspace,
                                                                                      table,
                                                                                      snapshotName,
                                                                                      includeSecondaryIndexFiles)
                                                            .build());
    }

    /**
     * Executes the clear snapshot request using the default retry policy and provided {@code instance}
     *
     * @param instance     the instance where the request will be executed
     * @param keyspace     the keyspace in Cassandra
     * @param table        the table name in Cassandra
     * @param snapshotName the name of the snapshot
     * @return a completable future for the request
     */
    public CompletableFuture<Void> clearSnapshot(SidecarInstance instance,
                                                 String keyspace,
                                                 String table,
                                                 String snapshotName)
    {
        return executor.executeRequestAsync(requestBuilder().singleInstanceSelectionPolicy(instance)
                                                            .clearSnapshotRequest(keyspace, table, snapshotName)
                                                            .build());
    }

    /**
     * Executes the create snapshot request using the default retry policy and provided {@code instance}
     *
     * @param instance     the instance where the request will be executed
     * @param keyspace     the keyspace in Cassandra
     * @param table        the table name in Cassandra
     * @param snapshotName the name of the snapshot
     * @return a completable future for the request
     */
    public CompletableFuture<Void> createSnapshot(SidecarInstance instance,
                                                  String keyspace,
                                                  String table,
                                                  String snapshotName)
    {
        return createSnapshot(instance, keyspace, table, snapshotName, null);
    }

    /**
     * Executes the create snapshot request using the default retry policy and provided {@code instance}
     *
     * @param instance     the instance where the request will be executed
     * @param keyspace     the keyspace in Cassandra
     * @param table        the table name in Cassandra
     * @param snapshotName the name of the snapshot
     * @param snapshotTTL  an optional time to live option for the snapshot (available since Cassandra 4.1+).
     *                     The TTL option must specify the units, for example 2d represents a TTL for 2 days;
     *                     1h represents a TTL of 1 hour, etc. Valid units are {@code d}, {@code h}, {@code s},
     *                     {@code ms}, {@code us}, {@code µs}, {@code ns}, and {@code m}.
     * @return a completable future for the request
     */
    public CompletableFuture<Void> createSnapshot(SidecarInstance instance,
                                                  String keyspace,
                                                  String table,
                                                  String snapshotName,
                                                  @Nullable String snapshotTTL)
    {
        return executor.executeRequestAsync(requestBuilder().retryPolicy(ignoreConflictRetryPolicy)
                                                            .singleInstanceSelectionPolicy(instance)
                                                            .createSnapshotRequest(keyspace, table,
                                                                                   snapshotName, snapshotTTL)
                                                            .build());
    }

    /**
     * Streams the specified {@code range} of an SSTable {@code componentName} for the given {@code keyspace},
     * {@code table} from an existing {@code snapshotName}, the stream is consumed by the
     * {@link StreamConsumer consumer}.
     *
     * @param instance       the instance where the request will be executed
     * @param keyspace       the keyspace in Cassandra
     * @param table          the table name in Cassandra
     * @param snapshotName   the name of the snapshot
     * @param componentName  the name of the SSTable component
     * @param range          the HTTP range for the request
     * @param streamConsumer the object that consumes the stream
     */
    public void streamSSTableComponent(SidecarInstance instance,
                                       String keyspace,
                                       String table,
                                       String snapshotName,
                                       String componentName,
                                       HttpRange range,
                                       StreamConsumer streamConsumer)
    {
        executor.streamRequest(requestBuilder()
                               .singleInstanceSelectionPolicy(instance)
                               .ssTableComponentRequest(keyspace, table, snapshotName, componentName, range)
                               .build(), streamConsumer);
    }

    /**
     * Uploads the SSTable to the provided {@code instance} using the default retry policy.
     *
     * @param instance      the instance where the request will be executed
     * @param keyspace      the keyspace in Cassandra
     * @param table         the table name in Cassandra
     * @param uploadId      the unique identifier for the upload
     * @param componentName the name of the SSTable component
     * @param digest        digest value to check integrity of SSTable component uploaded
     * @param filename      the path to the file to be uploaded
     * @return a completable future for the request
     */
    public CompletableFuture<Void> uploadSSTableRequest(SidecarInstance instance,
                                                        String keyspace,
                                                        String table,
                                                        String uploadId,
                                                        String componentName,
                                                        Digest digest,
                                                        String filename)
    {
        return executor.executeRequestAsync(requestBuilder().singleInstanceSelectionPolicy(instance)
                                                            .uploadSSTableRequest(keyspace,
                                                                                  table,
                                                                                  uploadId,
                                                                                  componentName,
                                                                                  digest,
                                                                                  filename)
                                                            .build());
    }

    /**
     * Executes the import SSTable request using the default retry policy and provided {@code instance}
     *
     * @param instance the instance where the request will be executed
     * @param keyspace the keyspace in Cassandra
     * @param table    the table name in Cassandra
     * @param uploadId the unique identifier for the upload
     * @param options  additional options for the import process
     * @return a completable future for the request
     */
    public CompletableFuture<SSTableImportResponse> importSSTableRequest(SidecarInstance instance,
                                                                         String keyspace,
                                                                         String table,
                                                                         String uploadId,
                                                                         ImportSSTableRequest.ImportOptions options)
    {
        Runnable customLog = () ->
                             LOGGER.info("Request to {} ACCEPTED but not yet complete - " +
                                         "will retry until success/failure. uploadId={}", instance, uploadId);
        RetryPolicy retryPolicy = new RunnableOnStatusCodeRetryPolicy(customLog,
                                                                      defaultRetryPolicy,
                                                                      HttpResponseStatus.ACCEPTED.code(),
                                                                      10);
        return executor.executeRequestAsync(requestBuilder().singleInstanceSelectionPolicy(instance)
                                                            .retryPolicy(retryPolicy)
                                                            .importSSTableRequest(keyspace, table, uploadId, options)
                                                            .build());
    }

    /**
     * Executes the clear upload session request using the default retry policy and provided {@code instance}
     *
     * @param instance the instance where the request will be executed
     * @param uploadId the unique identifier for the upload
     * @return a completable future for the request
     */
    public CompletableFuture<Void> cleanUploadSession(SidecarInstance instance, String uploadId)
    {
        return executor.executeRequestAsync(requestBuilder().singleInstanceSelectionPolicy(instance)
                                                            .cleanSSTableUploadSessionRequest(uploadId)
                                                            .build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<CreateRestoreJobResponsePayload> createRestoreJob(String keyspace,
                                                                               String table,
                                                                               CreateRestoreJobRequestPayload payload)
    {
        Objects.requireNonNull(payload, "payload cannot be null");
        return executor.executeRequestAsync(requestBuilder()
                                            .retryPolicy(new CreateRestoreJobRetryPolicy(defaultRetryPolicy))
                                            .request(new CreateRestoreJobRequest(keyspace, table, payload))
                                            .build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> updateRestoreJob(String keyspace,
                                                    String table,
                                                    UUID jobId,
                                                    UpdateRestoreJobRequestPayload payload)
    {
        return executor.executeRequestAsync(requestBuilder()
                                            .request(new UpdateRestoreJobRequest(keyspace, table, jobId, payload))
                                            .build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> abortRestoreJob(String keyspace, String table, UUID jobId)
    {
        return executor.executeRequestAsync(requestBuilder()
                                            .request(new AbortRestoreJobRequest(keyspace, table, jobId))
                                            .build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<RestoreJobSummaryResponsePayload> restoreJobSummary(String keyspace,
                                                                                 String table,
                                                                                 UUID jobId)
    {
        return executor.executeRequestAsync(requestBuilder()
                                            .request(new RestoreJobSummaryRequest(keyspace, table, jobId))
                                            .build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Void> createRestoreJobSlice(SidecarInstance instance,
                                                         String keyspace,
                                                         String table,
                                                         UUID jobId,
                                                         CreateSliceRequestPayload payload)
    {
        return executor.executeRequestAsync(requestBuilder()
                                            .singleInstanceSelectionPolicy(instance)
                                            .request(new CreateRestoreJobSliceRequest(keyspace, table, jobId, payload))
                                            .build());
    }

    /**
     * Returns a copy of the request builder with the default parameters configured for the client.
     *
     * <p>The request builder can be used to create the request, containing default values as depicted in the example
     * below:
     *
     * <pre>
     * RequestContext requestContext = client.requestBuilder()
     *                                       .request(new new NodeSettingsRequest())
     *                                       .retryPolicy(new NoRetryPolicy())
     *                                       .build();
     * </pre>
     *
     * <p>The example above will create a request to retrieve the node settings from a random Sidecar instance
     * in the cluster. It will use the {@code NoRetryPolicy} policy. A custom retry policy can encapsulate the
     * desired behavior of the client when dealing with specific status codes.
     *
     * @return a copy of the builder to prevent threads modifying the state of the builder
     */
    public RequestContext.Builder requestBuilder()
    {
        return baseBuilder.copy();
    }

    /**
     * @return the default {@link RetryPolicy} configured for the client
     */
    public RetryPolicy defaultRetryPolicy()
    {
        return defaultRetryPolicy;
    }

    /**
     * Returns a future with the expected instance of type {@code <T>} after executing the {@code request} and
     * processing it.
     *
     * @param context the request context
     * @param <T>     the expected type for the instance
     * @return a future with the expected instance of type {@code <T>} after executing the {@code request} and
     * processing it
     */
    public <T> CompletableFuture<T> executeRequestAsync(RequestContext context)
    {
        return executor.executeRequestAsync(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception
    {
        executor.close();
    }
}
