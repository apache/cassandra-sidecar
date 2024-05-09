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

package org.apache.cassandra.sidecar.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.testing.AbstractCassandraTestContext;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for integration test.
 * Start an in-jvm dtest cluster at the beginning of each test, and
 * teardown the cluster after each test.
 */
public abstract class IntegrationTestBase
{
    protected static final String TEST_KEYSPACE = "testkeyspace";
    protected static final int DEFAULT_RF = 3;
    private static final String TEST_TABLE_PREFIX = "testtable";
    private static final AtomicInteger TEST_TABLE_ID = new AtomicInteger(0);
    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    protected Vertx vertx;
    protected Server server;
    protected WebClient client;
    protected CassandraSidecarTestContext sidecarTestContext;
    protected Injector injector;

    @BeforeEach
    void setup(AbstractCassandraTestContext cassandraTestContext, TestInfo testInfo) throws InterruptedException
    {
        IntegrationTestModule integrationTestModule = new IntegrationTestModule();
        System.setProperty("cassandra.testtag", testInfo.getTestClass().get().getCanonicalName());
        System.setProperty("suitename", testInfo.getDisplayName() + ": " + cassandraTestContext.version);
        int clusterSize = cassandraTestContext.clusterSize();
        injector = Guice.createInjector(Modules.override(new MainModule()).with(integrationTestModule));
        vertx = injector.getInstance(Vertx.class);
        sidecarTestContext = CassandraSidecarTestContext.from(vertx, cassandraTestContext, DnsResolver.DEFAULT,
                                                              getNumInstancesToManage(clusterSize));

        integrationTestModule.setCassandraTestContext(sidecarTestContext);

        server = injector.getInstance(Server.class);
        client = WebClient.create(vertx);
        VertxTestContext context = new VertxTestContext();

        if (sidecarTestContext.isClusterBuilt())
        {
            MessageConsumer<JsonObject> cqlReadyConsumer = vertx.eventBus()
                                                                .localConsumer(ON_CASSANDRA_CQL_READY.address());
            cqlReadyConsumer.handler(message -> {
                cqlReadyConsumer.unregister();
                context.completeNow();
            });
        }

        client = WebClient.create(vertx);
        server.start()
              .onSuccess(s -> {
                  sidecarTestContext.registerInstanceConfigListener(this::healthCheck);
                  if (!sidecarTestContext.isClusterBuilt())
                  {
                      // Give everything a moment to get started and connected
                      vertx.setTimer(TimeUnit.SECONDS.toMillis(1), id1 -> context.completeNow());
                  }
              })
              .onFailure(context::failNow);

        context.awaitCompletion(5, TimeUnit.SECONDS);
    }

    /**
     * Some tests may want to "manage" fewer instances than the complete cluster.
     * Therefore, override this if your test wants to manage fewer than the complete cluster size.
     * The Sidecar will be configured to manage the first N instances in the cluster by instance number.
     * Defaults to the entire cluster.
     *
     * @param clusterSize the size of the cluster as defined by the integration test
     * @return the number of instances to manage; or -1 to let test framework to determine the cluster size at the runtime
     */
    protected int getNumInstancesToManage(int clusterSize)
    {
        return -1;
    }

    @AfterEach
    void tearDown() throws InterruptedException
    {
        CountDownLatch closeLatch = new CountDownLatch(1);
        client.close();
        server.close().onSuccess(res -> closeLatch.countDown());
        if (closeLatch.await(60, TimeUnit.SECONDS))
            logger.info("Close event received before timeout.");
        else
            logger.error("Close event timed out.");
        sidecarTestContext.close();
    }

    protected void testWithClient(VertxTestContext context, Consumer<WebClient> tester) throws Exception
    {
        testWithClient(context, true, tester);
    }

    protected void testWithClient(VertxTestContext context,
                                  boolean waitForCluster,
                                  Consumer<WebClient> tester)
    throws Exception
    {
        CassandraAdapterDelegate delegate = sidecarTestContext.instancesConfig()
                                                              .instanceFromId(1)
                                                              .delegate();

        assertThat(delegate).isNotNull();
        if (delegate.isNativeUp() || !waitForCluster)
        {
            tester.accept(client);
        }
        else
        {
            vertx.eventBus().localConsumer(ON_CASSANDRA_CQL_READY.address(), (Message<JsonObject> message) -> {
                if (message.body().getInteger("cassandraInstanceId") == 1)
                {
                    tester.accept(client);
                }
            });
        }

        // wait until the test completes
        assertThat(context.awaitCompletion(2, TimeUnit.MINUTES)).isTrue();
    }

    protected void createTestKeyspace()
    {
        createTestKeyspace(ImmutableMap.of("datacenter1", 1));
    }

    protected void createTestKeyspace(Map<String, Integer> rf)
    {
        int attempts = 1;
        ArrayList<Throwable> thrown = new ArrayList<>(5);
        while (attempts <= 5)
        {
            try
            {
                Session session = maybeGetSession();

                session.execute("CREATE KEYSPACE IF NOT EXISTS " + TEST_KEYSPACE +
                                " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', " +
                                generateRfString(rf) + " };");
                return;
            }
            catch (Throwable t)
            {
                thrown.add(t);
                logger.debug("Failed to create keyspace {} on attempt {}", TEST_KEYSPACE, attempts);
                attempts++;
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
        }
        RuntimeException rte = new RuntimeException("Could not create test keyspace after 5 attempts.");
        thrown.forEach(rte::addSuppressed);
        throw rte;
    }

    private String generateRfString(Map<String, Integer> dcToRf)
    {
        return dcToRf.entrySet().stream().map(e -> String.format("'%s':%d", e.getKey(), e.getValue()))
                     .collect(Collectors.joining(","));
    }

    protected QualifiedTableName createTestTable(String createTableStatement)
    {
        return createTestTable(TEST_TABLE_PREFIX, createTableStatement);
    }

    protected QualifiedTableName createTestTable(String tablePrefix, String createTableStatement)
    {
        Session session = maybeGetSession();
        QualifiedTableName tableName = uniqueTestTableFullName(tablePrefix);
        session.execute(String.format(createTableStatement, tableName));
        return tableName;
    }

    protected static void awaitLatchOrTimeout(CountDownLatch latch, long duration, TimeUnit timeUnit)
    {
        assertThat(Uninterruptibles.awaitUninterruptibly(latch, duration, timeUnit))
        .describedAs("Latch times out after " + duration + ' ' + timeUnit.name())
        .isTrue();
    }

    protected Session maybeGetSession()
    {
        Session session = sidecarTestContext.session();
        assertThat(session).isNotNull();
        return session;
    }

    private static QualifiedTableName uniqueTestTableFullName(String tablePrefix)
    {
        String unquotedTableName = tablePrefix + TEST_TABLE_ID.getAndIncrement();
        return new QualifiedTableName(new Name(TEST_KEYSPACE, Metadata.quoteIfNecessary(TEST_KEYSPACE)),
                                      new Name(unquotedTableName, Metadata.quoteIfNecessary(unquotedTableName)));
    }

    public List<Path> findChildFile(CassandraSidecarTestContext context, String hostname, String target)
    {
        InstanceMetadata instanceConfig = context.instancesConfig().instanceFromHost(hostname);
        List<String> parentDirectories = instanceConfig.dataDirs();

        return parentDirectories.stream().flatMap(s -> findChildFile(Paths.get(s), target).stream())
                                .collect(Collectors.toList());
    }

    private List<Path> findChildFile(Path path, String target)
    {
        try (Stream<Path> walkStream = Files.walk(path))
        {
            return walkStream.filter(p -> p.toString().endsWith(target)
                                          || p.toString().contains("/" + target + "/"))
                             .collect(Collectors.toList());
        }
        catch (IOException e)
        {
            return Collections.emptyList();
        }
    }

    private void healthCheck(InstancesConfig instancesConfig)
    {
        instancesConfig.instances()
                       .forEach(instanceMetadata -> instanceMetadata.delegate().healthCheck());
    }
}
