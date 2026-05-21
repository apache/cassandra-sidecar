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

package org.apache.cassandra.sidecar.livemigration;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationFilesVerificationRequest;
import org.apache.cassandra.sidecar.common.response.LiveMigrationFilesVerificationResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.utils.DigestVerifierFactory;
import org.apache.cassandra.sidecar.utils.SidecarClientProvider;

/**
 * Factory for creating {@link LiveMigrationFilesVerificationTask} instances used to verify file digests
 * between source and destination instances during live migration operation.
 */
@Singleton
public class LiveMigrationFilesVerificationTaskFactory
{
    private final Vertx vertx;
    private final SidecarClientProvider sidecarClientProvider;
    private final ExecutorPools executorPools;
    private final DigestVerifierFactory digestVerifierFactory;
    private final SidecarConfiguration sidecarConfiguration;

    @Inject
    public LiveMigrationFilesVerificationTaskFactory(Vertx vertx,
                                                     ExecutorPools executorPools,
                                                     SidecarConfiguration sidecarConfiguration,
                                                     SidecarClientProvider sidecarClientProvider,
                                                     DigestVerifierFactory digestVerifierFactory)
    {
        this.vertx = vertx;
        this.sidecarClientProvider = sidecarClientProvider;
        this.executorPools = executorPools;
        this.digestVerifierFactory = digestVerifierFactory;
        this.sidecarConfiguration = sidecarConfiguration;
    }

    public LiveMigrationTask<LiveMigrationFilesVerificationResponse> create(String id,
                                                                            String source,
                                                                            int port,
                                                                            LiveMigrationFilesVerificationRequest request,
                                                                            InstanceMetadata localInstanceMetadata)
    {
        return LiveMigrationFilesVerificationTask.builder()
                                                 .id(id)
                                                 .source(source)
                                                 .port(port)
                                                 .vertx(vertx)
                                                 .executorPools(executorPools)
                                                 .sidecarClient(sidecarClientProvider.get())
                                                 .digestVerifierFactory(digestVerifierFactory)
                                                 .liveMigrationConfiguration(sidecarConfiguration.liveMigrationConfiguration())
                                                 .request(request)
                                                 .instanceMetadata(localInstanceMetadata)
                                                 .build();
    }
}
