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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.config.SslConfiguration;
import org.apache.cassandra.sidecar.config.yaml.KeyStoreConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SslConfigurationImpl;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.server.SidecarServerEvents;
import org.apache.cassandra.testing.AbstractCassandraTestContext;
import org.apache.cassandra.testing.AuthMode;
import org.apache.cassandra.testing.utils.tls.CertificateBuilder;
import org.apache.cassandra.testing.utils.tls.CertificateBundle;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;
import static org.apache.cassandra.sidecar.testing.IntegrationTestModule.ADMIN_IDENTITY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for integration test.
 * Start an in-jvm dtest cluster at the beginning of each test, and
 * teardown the cluster after each test.
 */
public abstract class IntegrationTestBase
{
    private static final String IF_NOT_EXISTS = "IF NOT EXISTS";
    protected static final String TEST_KEYSPACE = "testkeyspace";
    protected static final int DEFAULT_RF = 3;
    protected static final String WITH_COMPACTION_DISABLED = " WITH COMPACTION = {\n" +
                                                             "   'class': 'SizeTieredCompactionStrategy', \n" +
                                                             "   'enabled': 'false' }";
    protected static final String TEST_TABLE_PREFIX = "testtable";
    protected static final String TEST_CLUSTER_PREFIX = "cluster";
    protected static final String DATA_CENTER_PREFIX = "datacenter";
    private static final AtomicInteger TEST_TABLE_ID = new AtomicInteger(0);
    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    protected Vertx vertx;
    protected Server server;
    @TempDir
    protected File tempDir;
    protected CertificateBundle ca;
    protected Path serverKeystorePath;
    protected Path clientKeystorePath;
    protected String clientKeystorePassword = "password";
    protected Path truststorePath;
    protected String truststorePassword = "password";
    protected WebClient client;
    protected CassandraSidecarTestContext sidecarTestContext;
    protected Injector injector;
    private final List<Throwable> testExceptions = new ArrayList<>();
    private Module testSpecificModule;
    private CountDownLatch schemaInitialized = new CountDownLatch(1);;

    @BeforeEach
    void setup(AbstractCassandraTestContext cassandraTestContext, TestInfo testInfo) throws Exception
    {
        testExceptions.clear();

        beforeSetup();

        ca = cassandraTestContext.ca;
        truststorePath = cassandraTestContext.truststorePath;
        serverKeystorePath = cassandraTestContext.serverKeystorePath;
        clientKeystorePath = clientKeystorePath(ADMIN_IDENTITY);

        IntegrationTestModule integrationTestModule = new IntegrationTestModule();
        integrationTestModule.setServerKeystorePath(serverKeystorePath);
        integrationTestModule.setTruststorePath(truststorePath);
        System.setProperty("cassandra.testtag", testInfo.getTestClass().get().getCanonicalName());
        System.setProperty("suitename", testInfo.getDisplayName() + ": " + cassandraTestContext.version);
        int clusterSize = cassandraTestContext.clusterSize();
        // list of modules that override the priors; hence order matters
        List<Module> modules = new ArrayList<>();
        modules.add(new MainModule());
        modules.add(integrationTestModule);
        if (testSpecificModule != null)
        {
            modules.add(testSpecificModule);
        }
        Module mergedModule = modules.stream().reduce((m1, m2) -> Modules.override(m1).with(m2)).get();
        injector = Guice.createInjector(mergedModule);
        vertx = injector.getInstance(Vertx.class);
        // register the handler for ON_SIDECAR_SCHEMA_INITIALIZED the earliest
        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED.address(),
                                       msg -> schemaInitialized.countDown());
        SslConfiguration sslConfig = cassandraTestContext.annotation.authMode().equals(AuthMode.MUTUAL_TLS)
                                     ? sslConfigWithClientKeystoreTruststore() : null;

        // When only SSL is enabled and mTLS is not enabled, we should not set keystore in SslConfig. Set a keystore
        // when mTLS is enabled
        if (cassandraTestContext.annotation.enableSsl() &&
            !cassandraTestContext.annotation.authMode().equals(AuthMode.MUTUAL_TLS))
        {
            sslConfig = sslConfigWithTruststore();
        }
        sidecarTestContext = CassandraSidecarTestContext.from(vertx, cassandraTestContext, DnsResolver.DEFAULT,
                                                              getInstancesToManage(clusterSize), sslConfig);
        integrationTestModule.setCassandraTestContext(sidecarTestContext);

        server = injector.getInstance(Server.class);
        VertxTestContext context = new VertxTestContext();

        boolean isClusterBuilt = sidecarTestContext.isClusterBuilt();
        if (isClusterBuilt)
        {
            MessageConsumer<JsonObject> cqlReadyConsumer = vertx.eventBus()
                                                                .localConsumer(ON_CASSANDRA_CQL_READY.address());
            cqlReadyConsumer.handler(message -> {
                cqlReadyConsumer.unregister();
                context.completeNow();
            });
        }

        client = mTLSClient();
        beforeServerStart();
        server.start()
              .onSuccess(s -> {
                  sidecarTestContext.registerInstanceConfigListener(this::healthCheck);
                  if (!isClusterBuilt)
                  {
                      // Give everything a moment to get started and connected
                      vertx.setTimer(TimeUnit.SECONDS.toMillis(1), id1 -> context.completeNow());
                  }
              })
              .onFailure(context::failNow);

        context.awaitCompletion(5, TimeUnit.SECONDS);

        // add a listener to refresh instance metadata when cluster is not yet built when starting server
        if (!isClusterBuilt)
        {
            cassandraTestContext.setClusterBuiltListener(cluster -> sidecarTestContext.refreshInstancesMetadata());
        }
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

    protected void beforeSetup()
    {
    }

    protected void beforeServerStart()
    {
    }

    protected void installTestSpecificModule(Module testSpecificModule)
    {
        this.testSpecificModule = testSpecificModule;
    }

    protected void waitForSchemaReady(long timeout, TimeUnit timeUnit)
    {
        awaitLatchOrTimeout(schemaInitialized, timeout, timeUnit, "Wait for schema initialization");
    }

    /**
     * Some tests may want to "manage" fewer instances than the complete cluster.
     * Therefore, override this if your test wants to manage fewer than the complete cluster size.
     * The Sidecar will be configured to manage the first N instances in the cluster by instance number.
     * Defaults to the entire cluster.
     *
     * @param clusterSize the size of the cluster as defined by the integration test
     * @return the instances to manage; or null to let test framework to determine the cluster size at the runtime
     */
    protected int[] getInstancesToManage(int clusterSize)
    {
        return null;
    }

    protected void testWithClient(Consumer<WebClient> tester)
    {
        testWithClient(true, tester);
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
        testWithClient(waitForCluster, tester);
         // wait until the test completes
        assertThat(context.awaitCompletion(2, TimeUnit.MINUTES)).isTrue();
    }

    protected void testWithClient(boolean waitForCluster,
                                  Consumer<WebClient> tester)
    {
        CassandraAdapterDelegate delegate = sidecarTestContext.instancesMetadata()
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
    }

    protected void createTestKeyspace()
    {
        createTestKeyspace(ImmutableMap.of(DATA_CENTER_PREFIX + 1, 1));
    }

    protected void createTestKeyspace(Map<String, Integer> rf)
    {
        createKeyspace(TEST_KEYSPACE, rf);
    }

    protected void createKeyspace(String keyspaceName, Map<String, Integer> rf)
    {
        int attempts = 1;
        ArrayList<Throwable> thrown = new ArrayList<>(5);
        while (attempts <= 5)
        {
            try
            {
                Session session = maybeGetSession();

                ResultSet rs = session.execute("CREATE KEYSPACE " + IF_NOT_EXISTS + " " + keyspaceName
                                               + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', " + generateRfString(rf) + " };");
                assertThat(rs.getExecutionInfo().isSchemaInAgreement())
                .describedAs("Schema agreement is not reached")
                .isTrue();
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

    /**
     * Protected helper method for creating test user-defined types (UDTs)
     *
     * @param name the name of the test UDT (without keyspace prefix)
     * @param schema the schema of the test UDT
     * @return the fully-qualified name of the created UDT (with keyspace prefix)
     */
    @NotNull
    @SuppressWarnings("UnusedReturnValue")
    protected String createTestUdt(@NotNull String name,
                                   @NotNull String schema)
    {
        return createTestUdt(name, schema, false);
    }

    /**
     * Protected helper method for creating test user-defined types (UDTs)
     */
    @NotNull
    @SuppressWarnings("SameParameterValue")
    protected String createTestUdt(@NotNull String name,
                                   @NotNull String schema,
                                   boolean ifNotExists)
    {
        String udt = TEST_KEYSPACE + "." + name;

        StringBuilder statement = new StringBuilder(1024);  // Specify capacity to prevent unnecessary resizing
        statement.append("CREATE TYPE ");
        statement.append(udt);
        if (ifNotExists)
        {
            statement.append(" ");
            statement.append(IF_NOT_EXISTS);
        }
        statement.append(" (");
        statement.append(schema);
        statement.append(");");

        Session session = maybeGetSession();  // Leave session open to enable its subsequent use by the test
        session.execute(statement.toString());

        return udt;
    }

    // similar to awaitLatchOrTimeout, it throws either test exceptions (due to startAsync failures) or timeout exception
    public void awaitLatchOrThrow(CountDownLatch latch, long duration, TimeUnit timeUnit, String latchName)
    {
        String hint = latchName == null ? "" : '(' + latchName + ')';
        boolean completed = Uninterruptibles.awaitUninterruptibly(latch, duration, timeUnit);
        if (completed)
        {
            return;
        }

        throwIfHasTestExceptions();
        throw new AssertionError("Latch " + hint + " times out after " + duration + ' ' + timeUnit.name());
    }

    public static void awaitLatchOrTimeout(CountDownLatch latch, long duration, TimeUnit timeUnit, String latchName)
    {
        String hint = latchName == null ? "" : '(' + latchName + ')';
        assertThat(Uninterruptibles.awaitUninterruptibly(latch, duration, timeUnit))
        .describedAs("Latch " + hint + " times out after " + duration + ' ' + timeUnit.name())
        .isTrue();
    }

    public static void awaitLatchOrTimeout(CountDownLatch latch, long duration, TimeUnit timeUnit)
    {
        awaitLatchOrTimeout(latch, duration, timeUnit, null);
    }

    protected Session maybeGetSession()
    {
        Session session = sidecarTestContext.session();
        assertThat(session).isNotNull();
        return session;
    }

    protected WebClient mTLSClient() throws Exception
    {
        Path clientKeystorePath = clientKeystorePath(ADMIN_IDENTITY, false);
        return createClient(clientKeystorePath, truststorePath);
    }

    protected void startAsync(String hints, Runnable runnable)
    {
        new Thread(() -> {
            try
            {
                runnable.run();
            }
            catch (Throwable t)
            {
                testExceptions.add(new RuntimeException(hints, t));
            }
        }).start();
    }

    protected void throwIfHasTestExceptions()
    {
        if (testExceptions.isEmpty())
            return;

        RuntimeException ex = new RuntimeException("Exceptions from async execution, i.e. IntegrationTestBase#startAsync. See the suppressed exceptions");
        for (Throwable t : testExceptions)
        {
            ex.addSuppressed(t);
        }
        throw ex;
    }

    protected void completeContextOrThrow(VertxTestContext context)
    {
        throwIfHasTestExceptions();
        context.completeNow();
    }

    private static QualifiedTableName uniqueTestTableFullName(String tablePrefix)
    {
        String uniqueTableName = tablePrefix + TEST_TABLE_ID.getAndIncrement();
        return new QualifiedTableName(new Name(Metadata.quoteIfNecessary(TEST_KEYSPACE)),
                                      new Name(Metadata.quoteIfNecessary(uniqueTableName)));
    }

    /**
     * Note: must disable compaction, otherwise the file tree can be mutated while walking and test becomes flaky
     * Append WITH_COMPACTION_DISABLED to the table create statement
     */
    public List<Path> findChildFile(CassandraSidecarTestContext context, String hostname, String keyspaceName, String target)
    {
        InstanceMetadata instanceConfig = context.instancesMetadata().instanceFromHost(hostname);
        List<String> parentDirectories = instanceConfig.dataDirs();

        return parentDirectories.stream()
                                .flatMap(s -> findChildFile(Paths.get(s, keyspaceName), target).stream())
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

    private void healthCheck(InstancesMetadata instancesMetadata)
    {
        instancesMetadata.instances()
                         .forEach(instanceMetadata -> instanceMetadata.delegate().healthCheck());
    }

    protected Path clientKeystorePath(String identity) throws Exception
    {
        return clientKeystorePath(identity, false);
    }

    protected Path clientKeystorePath(String identity, boolean expired) throws Exception
    {
        CertificateBuilder builder = new CertificateBuilder()
                            .subject("CN=Apache Cassandra, OU=ssl_test, O=Unknown, L=Unknown, ST=Unknown, C=Unknown")
                            .addSanDnsName("localhost")
                            .addSanIpAddress(subjectAlternativeNameIpAddress())
                            .addSanUriName(identity);
        if (expired)
        {
            builder.notAfter(Instant.now().minus(1, ChronoUnit.DAYS));
        }
        CertificateBundle clientKeystore = builder.buildIssuedBy(ca);
        return clientKeystore.toTempKeyStorePath(tempDir.toPath(), clientKeystorePassword.toCharArray(), clientKeystorePassword.toCharArray());
    }

    private SslConfiguration sslConfigWithClientKeystoreTruststore()
    {
        return SslConfigurationImpl
               .builder()
               .enabled(true)
               .keystore(new KeyStoreConfigurationImpl(clientKeystorePath.toAbsolutePath().toString(), clientKeystorePassword, "PKCS12"))
               .truststore(new KeyStoreConfigurationImpl(truststorePath.toAbsolutePath().toString(), truststorePassword, "PKCS12"))
               .build();
    }

    private SslConfiguration sslConfigWithTruststore()
    {
        return SslConfigurationImpl.builder()
                                   .enabled(true)
                                   .truststore(new KeyStoreConfigurationImpl(truststorePath.toAbsolutePath().toString(), truststorePassword, "PKCS12"))
                                   .build();
    }

    protected String subjectAlternativeNameIpAddress()
    {
        return "127.0.0.1";
    }

    protected WebClient createClient(Path clientKeystorePath, Path truststorePath)
    {
        WebClientOptions options = new WebClientOptions();
        options.setSsl(true);
        options.setKeyStoreOptions(new JksOptions().setPath(clientKeystorePath.toAbsolutePath().toString()).setPassword(clientKeystorePassword));
        options.setTrustStoreOptions(new JksOptions().setPath(truststorePath.toAbsolutePath().toString()).setPassword(truststorePassword));
        return WebClient.create(vertx, options);
    }
}
