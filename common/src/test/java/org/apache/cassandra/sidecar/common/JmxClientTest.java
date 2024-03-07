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

package org.apache.cassandra.sidecar.common;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RemoteObject;
import java.rmi.server.RemoteRef;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.commons.util.Preconditions;

import org.apache.cassandra.sidecar.common.exceptions.JmxAuthenticationException;
import sun.rmi.server.UnicastRef;
import sun.rmi.transport.LiveRef;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;


/***
 * In order to support multiple versions of Cassandra in the Sidecar, we would like to avoid depending directly on
 * any Cassandra code.
 * Additionally, whe would like to avoid copy/pasting the entire MBean interface classes into the Sidecar.
 * This test exists to prove out some assumptions about using matching sub-interfaces (or even functional interfaces)
 * to make JMX calls. This particular call happens to match the signature of the `importNewSSTables` method on
 * StorageServiceProxy in C* 4.0.
 */
class JmxClientTest
{
    private static final String objectName = "org.apache.cassandra.jmx:type=ExtendedImport";
    public static final int PROXIES_TO_TEST = 10_000;
    private static int port;
    private static JMXServiceURL serviceURL;
    private static StorageService importMBean;
    private static JMXConnectorServer jmxServer;
    private static MBeanServer mbs;
    private static Registry registry;
    @TempDir
    private static Path passwordFilePath;

    @BeforeAll
    public static void setUp() throws Exception
    {
        System.setProperty("java.rmi.server.hostname", "127.0.0.1");
        System.setProperty("java.rmi.server.randomIds", "true");
        String passwordFile = ResourceUtils.writeResourceToPath(JmxClientTest.class.getClassLoader(),
                                                                passwordFilePath,
                                                                "testJmxPassword.properties")
                                           .toAbsolutePath()
                                           .toString();
        Map<String, String> env = new HashMap<>();
        env.put("jmx.remote.x.password.file", passwordFile);
        registry = LocateRegistry.createRegistry(0); // dynamically allocate a port number
        mbs = ManagementFactory.getPlatformMBeanServer();

        port = determinePortNumber(registry);

        serviceURL = new JMXServiceURL("service:jmx:rmi://127.0.0.1:" + port
                                       + "/jndi/rmi://127.0.0.1:" + port + "/jmxrmi");
        jmxServer = JMXConnectorServerFactory.newJMXConnectorServer(serviceURL, env, mbs);
        jmxServer.start();
        importMBean = new StorageService();
        mbs.registerMBean(importMBean, new ObjectName(objectName));
    }

    @AfterAll
    public static void tearDown() throws Exception
    {
        jmxServer.stop();
        final ObjectName name = new ObjectName(objectName);
        if (mbs.isRegistered(name))
        {
            mbs.unregisterMBean(name);
        }
        UnicastRemoteObject.unexportObject(registry, true);
        registry = null;
    }

    @BeforeEach
    public void setup()
    {
        importMBean.shouldSucceed = true;
    }

    @RepeatedTest(1000)
//    @Test
    void testCanCallMethodWithoutEntireInterface() throws IOException
    {
        List<String> result;
        try (JmxClient client = JmxClient.builder()
                                         .jmxServiceURL(serviceURL)
                                         .role("controlRole")
                                         .password("password")
                                         .build())
        {
            result = client.proxy(Import.class, objectName)
                           .importNewSSTables(Sets.newHashSet("foo", "bar"), true,
                                              true, true, true, true,
                                              true);
        }
        assertThat(result.size()).isEqualTo(0);
    }

    @RepeatedTest(1000)
//    @Test
    void testCanCallMethodWithoutEntireInterfaceGetResults() throws IOException
    {
        importMBean.shouldSucceed = false;
        HashSet<String> srcPaths;
        List<String> failedDirs;
        try (JmxClient client = JmxClient.builder()
                                         .jmxServiceURL(serviceURL)
                                         .role("controlRole")
                                         .password("password")
                                         .build())
        {
            srcPaths = Sets.newHashSet("foo", "bar");
            failedDirs = client.proxy(Import.class, objectName)
                               .importNewSSTables(srcPaths, true,
                                                  true, true, true, true,
                                                  true);
        }
        assertThat(failedDirs.size()).isEqualTo(2);
        assertThat(failedDirs.toArray()).isEqualTo(srcPaths.toArray());
    }

    @RepeatedTest(1000)
//    @Test
    void testCallWithoutCredentialsFails() throws IOException
    {
        try (JmxClient client = JmxClient.builder().jmxServiceURL(serviceURL).build())
        {
            assertThatExceptionOfType(JmxAuthenticationException.class)
            .isThrownBy(() ->
                        client.proxy(Import.class, objectName)
                              .importNewSSTables(Sets.newHashSet("foo", "bar"),
                                                 true,
                                                 true,
                                                 true,
                                                 true,
                                                 true,
                                                 true))
            .withMessageContaining("Authentication failed! Credentials required");
        }
    }

    @RepeatedTest(1000)
//    @Test
    void testRoleSupplierThrows() throws IOException
    {
        String errorMessage = "bad role state!";
        Supplier<String> roleSupplier = () -> {
            throw new IllegalStateException(errorMessage);
        };
        testSupplierThrows(errorMessage, JmxClient.builder()
                                                  .jmxServiceURL(serviceURL)
                                                  .roleSupplier(roleSupplier)
                                                  .build());
    }

    @RepeatedTest(1000)
//    @Test
    void testPasswordSupplierThrows() throws IOException
    {
        String errorMessage = "bad password state!";
        Supplier<String> passwordSupplier = () -> {
            throw new IllegalStateException(errorMessage);
        };
        testSupplierThrows(errorMessage, JmxClient.builder()
                                                  .jmxServiceURL(serviceURL)
                                                  .roleSupplier(() -> "controlRole")
                                                  .passwordSupplier(passwordSupplier)
                                                  .build());
    }

    @RepeatedTest(1000)
//    @Test
    void testEnableSslSupplierThrows() throws IOException
    {
        String errorMessage = "bad ssl supplier state!";
        BooleanSupplier enableSslSupplier = () -> {
            throw new IllegalStateException(errorMessage);
        };
        testSupplierThrows(errorMessage, JmxClient.builder()
                                                  .jmxServiceURL(serviceURL)
                                                  .roleSupplier(() -> "controlRole")
                                                  .passwordSupplier(() -> "password")
                                                  .enableSslSupplier(enableSslSupplier)
                                                  .build());
    }

    @RepeatedTest(1000)
//    @Test
    void testRetryAfterAuthenticationFailureWithCorrectCredentials() throws IOException
    {
        AtomicInteger tryCount = new AtomicInteger(0);
        List<String> result;
        Supplier<String> passwordSupplier = () -> {
            if (tryCount.getAndIncrement() == 0)
            {
                // authentication fails on the first attempt
                return "bad password";
            }
            return "password";
        };
        try (JmxClient client = JmxClient.builder()
                                         .jmxServiceURL(serviceURL)
                                         .roleSupplier(() -> "controlRole")
                                         .passwordSupplier(passwordSupplier)
                                         .build())
        {
            // First attempt fails
            assertThatExceptionOfType(JmxAuthenticationException.class)
            .isThrownBy(() ->
                        client.proxy(Import.class, objectName)
                              .importNewSSTables(Sets.newHashSet("foo", "bar"),
                                                 true,
                                                 true,
                                                 true,
                                                 true,
                                                 true,
                                                 true))
            .withMessageContaining("Authentication failed! Invalid username or password");

            // second attempt succeeds after getting the correct password
            result = client.proxy(Import.class, objectName)
                           .importNewSSTables(Sets.newHashSet("foo", "bar"), true,
                                              true, true, true, true,
                                              true);
        }
        assertThat(result.size()).isEqualTo(0);
    }

    @RepeatedTest(1000)
//    @Test
    void testDisconnectReconnect() throws Exception
    {
        List<String> result;
        try (JmxClient client = JmxClient.builder()
                                         .jmxServiceURL(serviceURL)
                                         .role("controlRole")
                                         .password("password")
                                         .build())
        {
            assertThat(client.isConnected()).isFalse();
            result = client.proxy(Import.class, objectName)
                           .importNewSSTables(
                           Sets.newHashSet("foo", "bar"), true, true, true,
                           true, true,
                           true);
            assertThat(client.isConnected()).isTrue();
            assertThat(result.size()).isEqualTo(0);

            tearDown();
            setUp();

            result = client.proxy(Import.class, objectName)
                           .importNewSSTables(
                           Sets.newHashSet("foo", "bar"), true, true, true,
                           true, true,
                           true);
        }
        assertThat(result.size()).isEqualTo(0);
    }

    @RepeatedTest(1000)
//    @Test
    void testLotsOfProxies() throws IOException
    {
        try (JmxClient client = JmxClient.builder()
                                         .jmxServiceURL(serviceURL)
                                         .role("controlRole")
                                         .password("password")
                                         .build())
        {
            for (int i = 0; i < PROXIES_TO_TEST; i++)
            {
                List<String> result = client.proxy(Import.class, objectName)
                                            .importNewSSTables(
                                            Sets.newHashSet("foo", "bar"), true, true, true,
                                            true, true,
                                            true);
                assertThat(result).isNotNull();
            }
        }
    }

    @RepeatedTest(1000)
//    @Test
    void testConstructorWithHostPort() throws IOException
    {
        try (JmxClient client = JmxClient.builder()
                                         .host("127.0.0.1")
                                         .port(port)
                                         .roleSupplier(() -> "controlRole")
                                         .passwordSupplier(() -> "password")
                                         .build())
        {
            List<String> result = client.proxy(Import.class, objectName)
                                        .importNewSSTables(Sets.newHashSet("foo", "bar"), true,
                                                           true, true, true, true,
                                                           true);
            assertThat(result.size()).isEqualTo(0);
        }
    }

    /**
     * Simulates to C*'s `nodetool import` call
     */
    public interface Import
    {
        List<String> importNewSSTables(Set<String> srcPaths, boolean resetLevel, boolean clearRepaired,
                                       boolean verifySSTables, boolean verifyTokens, boolean invalidateCaches,
                                       boolean extendedVerify);
    }

    /**
     * Simulates the larger Storage Service MBean interface
     */
    public interface StorageServiceMBean
    {
        List<String> importNewSSTables(Set<String> srcPaths, boolean resetLevel, boolean clearRepaired,
                                       boolean verifySSTables, boolean verifyTokens, boolean invalidateCaches,
                                       boolean extendedVerify);

        void someOtherMethod(String helloString);
    }

    /**
     * An implementation of our mock StorageServiceMBean
     */
    public static class StorageService implements StorageServiceMBean
    {

        private static final Logger logger = Logger.getLogger(StorageService.class.getSimpleName());
        public boolean shouldSucceed = true;

        @Override
        public List<String> importNewSSTables(Set<String> srcPaths, boolean resetLevel, boolean clearRepaired,
                                              boolean verifySSTables, boolean verifyTokens,
                                              boolean invalidateCaches, boolean extendedVerify)
        {
            Preconditions.notNull(srcPaths, "Source Paths missing");
            if (shouldSucceed)
            {
                return Collections.emptyList();
            }
            return Arrays.asList(srcPaths.toArray(new String[0]));
        }

        @Override
        public void someOtherMethod(String helloString)
        {
            logger.info(helloString);
        }
    }

    private static void testSupplierThrows(String errorMessage, JmxClient jmxClient) throws IOException
    {
        try (JmxClient client = jmxClient)
        {
            assertThatExceptionOfType(JmxAuthenticationException.class)
            .isThrownBy(() ->
                        client.proxy(Import.class, objectName)
                              .importNewSSTables(Sets.newHashSet("foo", "bar"),
                                                 true,
                                                 true,
                                                 true,
                                                 true,
                                                 true,
                                                 true))
            .withMessageContaining(errorMessage);
        }
    }

    static int determinePortNumber(Registry registry)
    {
        if (registry instanceof RemoteObject)
        {
            RemoteRef ref = ((RemoteObject) registry).getRef();

            if (ref instanceof UnicastRef)
            {
                LiveRef liveRef = ((UnicastRef) ref).getLiveRef();
                return liveRef.getPort();
            }
        }
        throw new RuntimeException("Unable to determine port number");
    }
}
