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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.mockito.stubbing.Answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SidecarSchema} setup.
 */
@ExtendWith(VertxExtension.class)
public class SidecarSchemaTest
{
    private static final Logger logger = LoggerFactory.getLogger(SidecarSchemaTest.class);
    public static final String DEFAULT_SIDECAR_SCHEMA_KEYSPACE_NAME = "sidecar_internal";
    private static List<String> interceptedExecStmts = new ArrayList<>();
    private static List<String> interceptedPrepStmts = new ArrayList<>();

    private Vertx vertx;
    private SidecarSchema sidecarSchema;
    Server server;

    @BeforeEach
    public void setUp() throws InterruptedException
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule())
                                                        .with(Modules.override(new TestModule())
                                                                     .with(new SidecarSchemaTestModule())));
        this.vertx = injector.getInstance(Vertx.class);
        server = injector.getInstance(Server.class);
        sidecarSchema = injector.getInstance(SidecarSchema.class);

        VertxTestContext context = new VertxTestContext();
        server.start()
              .onSuccess(s -> context.completeNow())
              .onFailure(context::failNow);
        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    @AfterEach
    public void tearDown() throws InterruptedException
    {
        interceptedExecStmts.clear();
        interceptedPrepStmts.clear();
        CountDownLatch closeLatch = new CountDownLatch(1);
        vertx.close(result -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
    }

    @Test
    public void testSchemaInitOnStartup(VertxTestContext context)
    {
        sidecarSchema.startSidecarSchemaInitializer();
        context.verify(() -> {
            int maxWaitTime = 20; // about 10 seconds
            while (interceptedExecStmts.size() < 1 || !sidecarSchema.isInitialized())
            {
                if (maxWaitTime-- <= 0)
                {
                    context.failNow("test timeout");
                    break;
                }
                Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            }

            assertEquals(3, interceptedExecStmts.size());
            assertTrue(interceptedExecStmts.get(0).contains("CREATE KEYSPACE IF NOT EXISTS sidecar_internal"),
                       "Create keyspace should be executed the first");
            assertTrue(sidecarSchema.isInitialized());
            context.completeNow();
        });
    }

    /**
     * Test module override for {@link SidecarSchemaTest}
     */
    public static class SidecarSchemaTestModule extends AbstractModule
    {
        public final boolean intercept;

        public SidecarSchemaTestModule()
        {
            this(true);
        }

        public SidecarSchemaTestModule(boolean intercept)
        {
            this.intercept = intercept;
        }

        @Provides
        @Singleton
        public CQLSessionProvider cqlSessionProvider()
        {
            CQLSessionProvider cqlSession = mock(CQLSessionProvider.class);
            Session session = mock(Session.class, RETURNS_DEEP_STUBS);
            KeyspaceMetadata ks = mock(KeyspaceMetadata.class);
            when(ks.getTable(anyString())).thenReturn(null);
            when(session.getCluster()
                        .getMetadata()
                        .getKeyspace(anyString())).thenAnswer((Answer<KeyspaceMetadata>) invocation -> {
                if (DEFAULT_SIDECAR_SCHEMA_KEYSPACE_NAME.equals(invocation.getArgument(0)))
                {
                    return null;
                }
                return ks;
            });
            when(session.execute(any(String.class))).then(invocation -> {
                if (intercept)
                {
                    interceptedExecStmts.add(invocation.getArgument(0));
                }
                ResultSet rs = mock(ResultSet.class);
                ExecutionInfo ei = mock(ExecutionInfo.class);
                when(ei.isSchemaInAgreement()).thenReturn(true);
                when(rs.getExecutionInfo()).thenReturn(ei);
                return rs;
            });
            when(session.prepare(any(String.class))).then(invocation -> {
                if (intercept)
                {
                    interceptedPrepStmts.add(invocation.getArgument(0));
                }
                PreparedStatement ps = mock(PreparedStatement.class);
                BoundStatement stmt = mock(BoundStatement.class);
                when(ps.bind(any(Object[].class))).thenReturn(stmt);
                return ps;
            });
            when(cqlSession.get()).thenReturn(session);
            return cqlSession;
        }

        @Provides
        @Singleton
        public InstancesConfig instancesConfig()
        {
            InstanceMetadata instanceMeta = mock(InstanceMetadata.class);
            when(instanceMeta.stagingDir()).thenReturn("/tmp/staging"); // not an actual file
            InstancesConfig instancesConfig = mock(InstancesConfig.class);
            when(instancesConfig.instances()).thenReturn(Collections.singletonList(instanceMeta));
            when(instancesConfig.instanceFromHost(any())).thenReturn(instanceMeta);
            when(instancesConfig.instanceFromId(anyInt())).thenReturn(instanceMeta);
            return instancesConfig;
        }
    }

    private boolean hasElementContains(List<String> list, String substring)
    {
        for (String s : list)
        {
            if (s.contains(substring))
            {
                return true;
            }
        }
        return false;
    }
}
