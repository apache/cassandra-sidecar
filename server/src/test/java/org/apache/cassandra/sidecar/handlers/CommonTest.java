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

import java.util.Collections;
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
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Common test code to setup/teardown the server and populate test instances metadata
 */
public class CommonTest
{
    static final Logger LOGGER = LoggerFactory.getLogger(CommonTest.class);
    Vertx vertx;
    Server server;
    CassandraAdapterDelegate delegate = mock(CassandraAdapterDelegate.class);

    @BeforeEach
    void before() throws InterruptedException
    {
        Injector injector;
        Module testOverride = Modules.override(new TestModule())
                                     .with(new CommonTestModule());
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

    class CommonTestModule extends AbstractModule
    {
        @Provides
        @Singleton
        public InstancesMetadata instanceConfig()
        {
            int instanceId = 100;
            String host = "127.0.0.1";
            InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
            when(instanceMetadata.host()).thenReturn(host);
            when(instanceMetadata.port()).thenReturn(9042);
            when(instanceMetadata.id()).thenReturn(instanceId);
            when(instanceMetadata.stagingDir()).thenReturn("");
            when(instanceMetadata.delegate()).thenReturn(delegate);

            InstancesMetadata mockInstancesMetadata = mock(InstancesMetadata.class);
            when(mockInstancesMetadata.instances()).thenReturn(Collections.singletonList(instanceMetadata));
            when(mockInstancesMetadata.instanceFromId(instanceId)).thenReturn(instanceMetadata);
            when(mockInstancesMetadata.instanceFromHost(host)).thenReturn(instanceMetadata);

            return mockInstancesMetadata;
        }
    }
}
