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

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.Session;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
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
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.sidecar.common.response.StreamStatsResponse;
import org.apache.cassandra.sidecar.common.response.data.StreamProgressStats;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the stream stats endpoint with cassandra container.
 */
@ExtendWith(VertxExtension.class)
public class StreamStatsIntegrationTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(numDataDirsPerInstance = 4, nodesPerDc = 2, network = true, buildCluster = false)
    void streamStatsTest(VertxTestContext context, ConfigurableCassandraTestContext cassandraTestContext) throws InterruptedException
    {
        BBHelperDecommissioningNode.reset();
        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(
        builder -> builder.withInstanceInitializer(BBHelperDecommissioningNode::install));
        IUpgradeableInstance node = cluster.get(2);
        IUpgradeableInstance seed = cluster.get(1);

        createTestKeyspace();
        createTestTableAndPopulate();

        startAsync("Decommission node" + node.config().num(),
                   () -> node.nodetoolResult("decommission", "--force").asserts().success());
        AtomicBoolean hasStats = new AtomicBoolean(false);
        AtomicBoolean dataReceived = new AtomicBoolean(false);

        // Wait until nodes have reached expected state
        awaitLatchOrThrow(BBHelperDecommissioningNode.transientStateStart, 2, TimeUnit.MINUTES, "transientStateStart");

        ClusterUtils.awaitRingState(seed, node, "Leaving");
        BBHelperDecommissioningNode.transientStateEnd.countDown();

        // optimal no. of attempts to poll for stats to capture streaming stats during node decommissioning
        for (int i = 0; i < 20; i++)
        {
            startAsync("Request-" + i , () -> {
                try
                {
                    streamStats(context, hasStats, dataReceived);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            });

            Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
        }

        assertThat(hasStats).isTrue();
        assertThat(dataReceived).isTrue();
        context.completeNow();
        context.awaitCompletion(2, TimeUnit.MINUTES);
    }

    private void streamStats(VertxTestContext context, AtomicBoolean hasStats, AtomicBoolean dataReceived) throws Exception
    {
        String testRoute = "/api/v1/cassandra/stats/streams";
        testWithClient(context, client -> {
            BBHelperDecommissioningNode.transientStateEnd.countDown();
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .send(context.succeeding(response -> {
                       assertStreamStatsResponseOK(response, hasStats, dataReceived);
                  }));
        });
    }

    void assertStreamStatsResponseOK(HttpResponse<Buffer> response, AtomicBoolean hasStats, AtomicBoolean dataReceived)
    {
        StreamStatsResponse streamStatsResponse = response.bodyAsJson(StreamStatsResponse.class);
        assertThat(streamStatsResponse).isNotNull();
        StreamProgressStats streamProgress = streamStatsResponse.streamProgressStats();
        assertThat(streamProgress).isNotNull();
        if (streamProgress.totalFilesToReceive() > 0)
        {
            hasStats.set(true);
            if (streamProgress.totalFilesToReceive() == streamProgress.totalFilesReceived() &&
                streamProgress.totalFilesReceived() > 0)
            {
                dataReceived.set(true);
                assertThat(streamProgress.totalBytesToReceive()).isEqualTo(streamProgress.totalBytesReceived());
                assertThat(streamProgress.totalBytesReceived()).isGreaterThan(0);
            }
        }
    }

    QualifiedTableName createTestTableAndPopulate()
    {
        QualifiedTableName tableName = createTestTable(
        "CREATE TABLE %s ( \n" +
        "  race_year int, \n" +
        "  race_name text, \n" +
        "  cyclist_name text, \n" +
        "  rank int, \n" +
        "  PRIMARY KEY ((race_year, race_name), rank) \n" +
        ");");
        Session session = maybeGetSession();

        session.execute("CREATE INDEX ryear ON " + tableName + " (race_year);");

        for (int i = 1; i <= 1000; i++)
        {
            session.execute("INSERT INTO " + tableName + " (race_year, race_name, rank, cyclist_name) " +
                            "VALUES (2015, 'Tour of Japan - Stage 4 - Minami > Shinshu', " + i + ", 'Benjamin PRADES');");
        }
        return tableName;
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
            // Test case involves 5 node cluster with 1 leaving node
            // We intercept the shutdown of the leaving node (2) to validate token ranges
            if (nodeNumber == 2)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperDecommissioningNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transientStateStart.countDown();
            awaitLatchOrTimeout(transientStateEnd, 2, TimeUnit.MINUTES, "transientStateEnd");
            orig.call();
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(1);
            transientStateEnd = new CountDownLatch(1);
        }
    }
}
