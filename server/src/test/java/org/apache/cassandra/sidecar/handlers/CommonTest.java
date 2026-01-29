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

package org.apache.cassandra.sidecar.handlers;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Common test code to setup/teardown the server and populate test instances metadata
 */
public class CommonTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CommonTest.class);
    Vertx vertx;
    Server server;
    CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule())
                                     .with(new CommonTestModule(delegate));
        injector = Guice.createInjector(Modules.override(SidecarModules.all())
                                               .with(testOverride));
        vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void after() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            LOGGER.info("Close event received before timeout.");
        else
            LOGGER.error("Close event timed out.");
    }

    /**
     * Common test module to provide default mocks for testing
     */
    public static class CommonTestModule extends AbstractModule
    {
        protected static final int DEFAULT_INSTANCE_ID = 100;
        protected static final String DEFAULT_HOST = "127.0.0.1";
        protected static final int DEFAULT_PORT = 9042;

        protected CassandraAdapterDelegate delegate;
        protected StorageOperations storageOperations;

        /**
         * Test module with custom delegate
         *
         * @param delegate the CassandraAdapterDelegate mock to use
         */
        public CommonTestModule(CassandraAdapterDelegate delegate)
        {
            this.delegate = delegate;
            this.storageOperations = delegate.storageOperations();
        }

        /**
         * Test module with custom StorageOperations
         *
         * @param storageOperations the StorageOperations mock to use
         */
        public CommonTestModule(StorageOperations storageOperations)
        {
            this.delegate = mock(CassandraAdapterDelegate.class);
            this.storageOperations = storageOperations;
            when(delegate.storageOperations()).thenReturn(storageOperations);
        }

        @Provides
        @Singleton
        public InstancesMetadata instanceConfig()
        {
            InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
            when(instanceMetadata.host()).thenReturn(DEFAULT_HOST);
            when(instanceMetadata.port()).thenReturn(DEFAULT_PORT);
            when(instanceMetadata.id()).thenReturn(DEFAULT_INSTANCE_ID);
            when(instanceMetadata.stagingDir()).thenReturn("");
            when(instanceMetadata.delegate()).thenReturn(delegate);

            InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
            when(mockInstancesMetadata.instances()).thenReturn(List.of(instanceMetadata));
            when(mockInstancesMetadata.instanceFromId(DEFAULT_INSTANCE_ID)).thenReturn(instanceMetadata);
            when(mockInstancesMetadata.instanceFromHost(DEFAULT_HOST)).thenReturn(instanceMetadata);

            return mockInstancesMetadata;
        }
    }
}
