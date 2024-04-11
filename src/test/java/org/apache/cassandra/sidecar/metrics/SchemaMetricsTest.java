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

package org.apache.cassandra.sidecar.metrics;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.SidecarSchemaTest;
import org.apache.cassandra.sidecar.db.schema.SidecarInternalKeyspace;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.exceptions.SidecarSchemaModificationException;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;

import static org.apache.cassandra.sidecar.AssertionUtils.loopAssert;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests metrics emitted for {@link SidecarSchema}
 */
public class SchemaMetricsTest
{
    private static final Logger logger = LoggerFactory.getLogger(SidecarSchemaTest.class);
    private Vertx vertx;
    private SidecarSchema sidecarSchema;
    private SchemaMetrics metrics;
    Server server;

    @BeforeEach
    public void setUp() throws InterruptedException
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule())
                                                        .with(Modules.override(new TestModule())
                                                                     .with(new SchemaFailureSimulateModule())));
        this.vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        sidecarSchema = injector.getInstance(SidecarSchema.class);
        metrics = injector.getInstance(SchemaMetrics.class);

        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    public void tearDown() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        registry().removeMatching((name, metric) -> true);
        vertx.close(result -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }

    @Test
    public void testSchemaModificationFailure()
    {
        sidecarSchema.startSidecarSchemaInitializer();
        loopAssert(3, () -> {
            assertThat(metrics.failedInitializations.metric.getValue())
            .isGreaterThanOrEqualTo(1);
        });
    }

    /**
     * Test module override for {@link SchemaMetricsTest}
     */
    public static class SchemaFailureSimulateModule extends AbstractModule
    {
        @Provides
        @Singleton
        public CQLSessionProvider cqlSessionProvider()
        {
            CQLSessionProvider cqlSession = mock(CQLSessionProvider.class);
            Session session = mock(Session.class);
            when(cqlSession.get()).thenReturn(session);
            return cqlSession;
        }

        @Provides
        @Singleton
        public SidecarSchema sidecarSchema(Vertx vertx,
                                           ExecutorPools executorPools,
                                           SidecarConfiguration configuration,
                                           SchemaMetrics metrics,
                                           CQLSessionProvider cqlSessionProvider)
        {
            SidecarInternalKeyspace sidecarInternalKeyspace = mock(SidecarInternalKeyspace.class);
            when(sidecarInternalKeyspace.initialize(any()))
            .thenThrow(new SidecarSchemaModificationException("Simulated failure",
                                                              new RuntimeException("Simulated exception")));
            return new SidecarSchema(vertx, executorPools, configuration,
                                     sidecarInternalKeyspace, cqlSessionProvider, metrics);
        }
    }
}
