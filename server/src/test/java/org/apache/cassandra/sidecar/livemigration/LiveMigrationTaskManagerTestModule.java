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

import java.util.List;

import com.google.inject.AbstractModule;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.apache.cassandra.sidecar.common.request.LiveMigrationFilesVerificationRequest;
import org.apache.cassandra.sidecar.common.response.LiveMigrationDataCopyResponse;
import org.apache.cassandra.sidecar.common.response.LiveMigrationFilesVerificationResponse;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.handlers.livemigration.FakeLiveMigrationTask;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Common Guice test module for live migration task manager tests.
 * Provides shared mock configurations for InstancesMetadata, SidecarConfiguration,
 * and other common dependencies used across different task manager tests.
 */
class LiveMigrationTaskManagerTestModule extends AbstractModule
{
    static final String SOURCE_1 = "source1";
    static final String SOURCE_2 = "source2";
    static final int DEST_1_ID = 200001;
    static final int DEST_2_ID = 200002;
    static final int DEST_3_ID = 200003;
    static final String DESTINATION_1 = "destination1";
    static final String DESTINATION_2 = "destination2";
    static final String DESTINATION_3 = "destination3";
    static final int CONCURRENT_FILE_REQUESTS = 5;

    protected final LiveMigrationTaskFactory mockLiveMigrationTaskFactory = mock(LiveMigrationTaskFactory.class);
    protected final LiveMigrationFilesVerificationTaskFactory mockFilesVerificationTaskFactory = mock(LiveMigrationFilesVerificationTaskFactory.class);
    protected final SidecarConfiguration mockSidecarConfiguration = mock(SidecarConfiguration.class);
    protected final ServiceConfiguration mockServiceConfiguration = mock(ServiceConfiguration.class);
    protected final LiveMigrationConfiguration mockLiveMigrationConfiguration = mock(LiveMigrationConfiguration.class);
    protected final LiveMigrationMap mockLiveMigrationMap = mock(LiveMigrationMap.class);
    protected final InstanceMetadata mockDest1InstanceMeta = mock(InstanceMetadata.class);
    protected final InstanceMetadata mockDest2InstanceMeta = mock(InstanceMetadata.class);
    protected final InstanceMetadata mockDest3InstanceMeta = mock(InstanceMetadata.class);
    protected final InstanceMetadata mockSourceInstanceMeta = mock(InstanceMetadata.class);
    protected final InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
    protected final Vertx vertx; // = Vertx.vertx();

    LiveMigrationTaskManagerTestModule(Vertx vertx)
    {
        this.vertx = vertx;
    }

    @Override
    protected void configure()
    {
        // Bind common dependencies
        bind(Vertx.class).toInstance(vertx);
        bind(SidecarConfiguration.class).toInstance(mockSidecarConfiguration);
        bind(InstancesMetadata.class).toInstance(mockInstancesMetadata);
        bind(LiveMigrationMap.class).toInstance(mockLiveMigrationMap);
        bind(LiveMigrationTaskFactory.class).toInstance(mockLiveMigrationTaskFactory);
        bind(LiveMigrationFilesVerificationTaskFactory.class).toInstance(mockFilesVerificationTaskFactory);

        // Configure SidecarConfiguration mocks
        when(mockSidecarConfiguration.serviceConfiguration()).thenReturn(mockServiceConfiguration);
        when(mockServiceConfiguration.port()).thenReturn(9043);
        when(mockSidecarConfiguration.liveMigrationConfiguration()).thenReturn(mockLiveMigrationConfiguration);
        when(mockLiveMigrationConfiguration.maxConcurrentFileRequests()).thenReturn(CONCURRENT_FILE_REQUESTS);

        // Configure InstanceMetadata mocks
        List<String> twoDataDirs = List.of("/data1", "/data2");
        configureInstanceMetadata(mockDest1InstanceMeta, DESTINATION_1, DEST_1_ID, twoDataDirs);
        configureInstanceMetadata(mockDest2InstanceMeta, DESTINATION_2, DEST_2_ID, twoDataDirs);
        configureInstanceMetadata(mockDest3InstanceMeta, DESTINATION_3, DEST_3_ID, twoDataDirs);
        configureInstanceMetadata(mockSourceInstanceMeta, SOURCE_1, 0, List.of("/data1"));

        when(mockInstancesMetadata.instanceFromHost(DESTINATION_1)).thenReturn(mockDest1InstanceMeta);
        when(mockInstancesMetadata.instanceFromHost(DESTINATION_2)).thenReturn(mockDest2InstanceMeta);
        when(mockInstancesMetadata.instanceFromHost(DESTINATION_3)).thenReturn(mockDest3InstanceMeta);
        when(mockInstancesMetadata.instanceFromHost(SOURCE_1)).thenReturn(mockSourceInstanceMeta);

        // Configure LiveMigrationMap
        when(mockLiveMigrationMap.getSource(anyString())).thenReturn(Future.succeededFuture(SOURCE_1));

        // Configure default factory behaviors
        configureDataCopyTaskFactory();
        configureFilesVerificationTaskFactory();
    }

    private void configureInstanceMetadata(InstanceMetadata instanceMeta, String hostName, int id, List<String> dataDirs)
    {
        when(instanceMeta.host()).thenReturn(hostName);
        when(instanceMeta.id()).thenReturn(id);
        when(instanceMeta.dataDirs()).thenReturn(dataDirs);
        when(instanceMeta.delegate()).thenReturn(mock(CassandraAdapterDelegate.class));
    }

    private void configureDataCopyTaskFactory()
    {
        when(mockLiveMigrationTaskFactory.create(anyString(), any(LiveMigrationDataCopyRequest.class), anyString(), anyInt(), any(InstanceMetadata.class)))
        .thenAnswer(invocation -> {
            String id = invocation.getArgument(0);
            LiveMigrationDataCopyRequest request = invocation.getArgument(1);
            String source = invocation.getArgument(2);
            int port = invocation.getArgument(3);

            List<LiveMigrationDataCopyResponse.Status> statusList =
            List.of(new LiveMigrationDataCopyResponse.Status(0, "SUCCESS", 1000L, 1, 1, 1, 1, 0, 1000L));
            LiveMigrationDataCopyResponse taskResponse = new LiveMigrationDataCopyResponse(id, source, port, request, statusList);
            return new FakeLiveMigrationTask(taskResponse);
        });
    }

    private void configureFilesVerificationTaskFactory()
    {
        when(mockFilesVerificationTaskFactory.create(anyString(), anyString(), anyInt(), any(LiveMigrationFilesVerificationRequest.class), any(InstanceMetadata.class)))
        .thenAnswer(invocation -> {
            String id = invocation.getArgument(0);
            String source = invocation.getArgument(1);
            int port = invocation.getArgument(2);

            LiveMigrationFilesVerificationResponse response = new LiveMigrationFilesVerificationResponse(
            id, "MD5", "COMPLETED", source, port, 0, 0, CONCURRENT_FILE_REQUESTS, 0, 0, CONCURRENT_FILE_REQUESTS, 0
            );
            return new FakeFilesVerificationTask(response);
        });
    }
}
