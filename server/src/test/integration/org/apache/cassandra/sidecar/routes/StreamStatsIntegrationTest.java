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

package org.apache.cassandra.sidecar.routes;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.sidecar.common.response.StreamStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.StreamsProgressStats;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.server.utils.ThrowableUtils;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests the stream stats endpoint with cassandra container.
 */
public class StreamStatsIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(numDataDirsPerInstance = 4, nodesPerDc = 2, network = true)
    void streamStatsTest(CassandraTestContext cassandraTestContext)
    {
//        BBHelperDecommissioningNode.reset();
        UpgradeableCluster cluster = cassandraTestContext.cluster();
//        cassandraTestContext.configureAndStartCluster(
//        builder -> builder.withInstanceInitializer(BBHelperDecommissioningNode::install));

        createTestKeyspace(Map.of("datacenter1", 2));
        QualifiedTableName tableName = createTestTable(
        "CREATE TABLE %s ( \n" +
        "  race_year int, \n" +
        "  race_name text, \n" +
        "  cyclist_name text, \n" +
        "  rank int, \n" +
        "  PRIMARY KEY ((race_year, race_name), rank) \n" +
        ");");
        // craft inconsistency for repair
        populateDataAtNode2Only(cluster, tableName);

        // Poll stream stats while repair is running in the background.
        CountDownLatch testStart = new CountDownLatch(1);
        IUpgradeableInstance node = cluster.get(1);
        AtomicReference<Throwable> nodetoolError = new AtomicReference<>();
        startAsync("Repairing node" + node.config().num(),
                   () -> {
                       Uninterruptibles.awaitUninterruptibly(testStart);
                       try
                       {
                           node.nodetoolResult("repair", tableName.keyspace(), tableName.tableName(), "--full").asserts().success();
                       }
                       catch (Throwable cause)
                       {
                           nodetoolError.set(cause);
                       }
                   });

        TestState testState = new TestState();
        testStart.countDown();
        loopAssert(5, 100, () -> {
            if (nodetoolError.get() != null)
            {
                fail("Nodetool command failed", nodetoolError.get());
            }
            streamStats(testState);
            testState.assertCompletion();
        });
    }

    private void streamStats(TestState testState)
    {
        String testRoute = "/api/v1/cassandra/stats/streams";
        HttpResponse<Buffer> resp;
        resp = getBlocking(client.get(server.actualPort(), "127.0.0.1", testRoute)
                                 .send());
        assertStreamStats(resp, testState);
    }

    void assertStreamStats(HttpResponse<Buffer> response, TestState testState)
    {
        assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());
        StreamStatsResponse streamStatsResponse = response.bodyAsJson(StreamStatsResponse.class);
        assertThat(streamStatsResponse).isNotNull();
        StreamsProgressStats streamProgress = streamStatsResponse.streamsProgressStats();
        assertThat(streamProgress).isNotNull();
        testState.update(streamProgress);
    }

    static class TestState
    {
        ObjectMapper mapper = new ObjectMapper();
        StreamsProgressStats lastStats;
        boolean streamStarted = false, streamCompleted = false;

        void update(StreamsProgressStats streamProgress)
        {
            lastStats = streamProgress;
            if (streamProgress.totalFilesToReceive() > 0)
            {
                streamStarted = true;
            }
            if (streamProgress.totalFilesReceived() == streamProgress.totalFilesToReceive())
            {
                streamCompleted = true;
            }
        }

        void assertCompletion()
        {
            String json = ThrowableUtils.propagate(() -> mapper.writeValueAsString(lastStats));
            assertThat(streamStarted)
            .describedAs("Expecting to have non-empty stream stats. last stats: " + json)
            .isTrue();
            assertThat(streamCompleted)
            .describedAs("Expecting to complete. last stats: " + json)
            .isTrue();
        }
    }

    void populateDataAtNode2Only(UpgradeableCluster cluster, QualifiedTableName tableName)
    {
        for (int i = 1; i <= 50; i++)
        {
            cluster.get(2).executeInternal("INSERT INTO " + tableName + " (race_year, race_name, rank, cyclist_name) " +
                                           "VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', " + i + ", 'Benjamin PRADES');");
            cluster.get(2).flush(TEST_KEYSPACE);
        }
    }

    /**
     * ByteBuddy Helper for decommissioning node
     */
    public static class BBHelperDecommissioningNode
    {
        static CountDownLatch transientStateStart = new CountDownLatch(1);
        static CountDownLatch transientStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            if (nodeNumber == 2)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.streaming.StreamCoordinator")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("connectAllStreamSessions"))
                               .intercept(MethodDelegation.to(BBHelperDecommissioningNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void connectAllStreamSessions(@SuperCall Callable<StreamOperation> orig) throws Exception
        {
            transientStateStart.countDown();
            Uninterruptibles.sleepUninterruptibly(3, TimeUnit.SECONDS);
            orig.call();
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(1);
            transientStateEnd = new CountDownLatch(1);
        }
    }
}
