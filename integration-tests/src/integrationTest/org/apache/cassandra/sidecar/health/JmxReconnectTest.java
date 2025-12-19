/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.health;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.rmi.NoSuchObjectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import javax.management.remote.rmi.RMIConnector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.api.condition.JRE;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.response.RingResponse;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static net.bytebuddy.matcher.ElementMatchers.isConstructor;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.none;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that validates JMX reconnection handling to prevent deadlocks during connection recovery.
 */
public class JmxReconnectTest extends SharedClusterSidecarIntegrationTestBase
{
    @Override
    protected void initializeSchemaForTest()
    {
        // NO schema needed
    }

    static
    {
        Instrumentation inst = ByteBuddyAgent.install();

        // Make Holder + CtorAdvice visible to bootstrap
        File bootstrapJar = null;
        try
        {
            bootstrapJar = createBootstrapJar();
            inst.appendToBootstrapClassLoaderSearch(new JarFile(bootstrapJar));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        // Intercept RMIConnector's getAttribute method
        new AgentBuilder.Default()
        .ignore(none())
        .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
        .type(named("javax.management.remote.rmi.RMIConnector$RemoteMBeanServerConnection"))
        .transform((builder, td, cl, m, i) ->
                   builder.visit(Advice.to(GetAttributeAdvice.class).on(named("getAttribute")))
        )
        .installOn(inst);

        // Intercept RMIClientCommunicatorAdmin's constructor to capture the object
        new AgentBuilder.Default()
        .ignore(none())
        .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
        .type(named("javax.management.remote.rmi.RMIConnector$RMIClientCommunicatorAdmin"))
        .transform((builder, td, cl, m, i) ->
                   builder.visit(Advice.to(ConstructorAdvice.class).on(isConstructor()))
        )
        .installOn(inst);
    }

    /**
     * Holder for a list of objects.
     */
    // Must not reference any non-JDK types!
    public static class ObjectHolder
    {
        public static List<Object> instance = new ArrayList<>();

        public static void set(Object o)
        {
            instance.add(o);
        }
    }

    /**
     * Interceptor for constructor of RMIConnector$RMIClientCommunicatorAdmin.
     */
    public static class ConstructorAdvice
    {
        @Advice.OnMethodExit
        static void after(@Advice.This Object self)
        {
            ObjectHolder.set(self);
        }
    }

    /**
     * Interceptor for getAttribute method of RMIConnector.
     */
    public static class GetAttributeAdvice
    {
        // Call RMIClientCommunicatorAdmin.gotIOException when RMIConnector.getAttribute is called
        // and the stack trace has doStart method call.
        @Advice.OnMethodEnter
        static void before(@Advice.This Object self)
        {
            try
            {
                String str = Arrays.stream(Thread.currentThread().getStackTrace()).map(Object::toString).collect(Collectors.joining(System.lineSeparator()));
                if (str.contains("doStart"))
                {
                    // Get reference of RMIConnector outer class
                    Field this0field = self.getClass().getDeclaredField("this$0");
                    this0field.setAccessible(true);
                    RMIConnector rmiConnector = (RMIConnector) this0field.get(self);

                    // Get RMIClientCommunicatorAdmin object of RMIConnector
                    Field commAdminField = RMIConnector.class.getDeclaredField("communicatorAdmin");
                    commAdminField.setAccessible(true);
                    Object obj = commAdminField.get(rmiConnector);

                    // Call RMIClientCommunicatorAdmin.gotIOException
                    Method m = obj.getClass().getDeclaredMethod("gotIOException", IOException.class);
                    m.setAccessible(true);
                    m.invoke(obj, new NoSuchObjectException("injected"));
                }
            }
            catch (Exception ex)
            {
                throw new RuntimeException(ex);
            }
        }
    }

    private static File createBootstrapJar() throws Exception
    {
        File jar = File.createTempFile("bb-bootstrap", ".jar");
        jar.deleteOnExit();

        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar)))
        {
            addClassToJar(jos, ObjectHolder.class);
            addClassToJar(jos, ConstructorAdvice.class);
            addClassToJar(jos, GetAttributeAdvice.class);
        }

        return jar;
    }

    private static void addClassToJar(JarOutputStream jos, Class<?> clazz) throws IOException
    {
        String name = clazz.getName().replace('.', '/') + ".class";
        jos.putNextEntry(new JarEntry(name));
        try (InputStream is = clazz.getClassLoader().getResourceAsStream(name))
        {
            Assertions.assertNotNull(is);
            is.transferTo(jos);
        }
        jos.closeEntry();
    }

    /**
     * Tests JMX reconnection handling to prevent deadlocks in ClientCommunicatorAdmin.restart.
     * <p>
     * This test simulates a specific deadlock scenario that can occur during JMX reconnection:
     * <ol>
     *   <li>JMX connection undergoes reconnection process</li>
     *   <li>A notification handler for connection status changes executes JMX calls during reconnection</li>
     *   <li>The JMX call fails with an IOException</li>
     *   <li>This sequence creates a potential deadlock in ClientCommunicatorAdmin.restart</li>
     * </ol>
     * <p>
     * Jira link for the issue: <a href="https://issues.apache.org/jira/browse/CASSSIDECAR-390">CASSSIDECAR-390</a>
     * The test uses bytecode instrumentation to inject IOException failures at specific points in the
     * JMX reconnection flow, then validates that the system can recover and continue processing
     * JMX requests successfully.
     * <p>
     * <b>Note:</b> This test is disabled on JDK 17+ because bytecode instrumentation requires access
     * to internal JMX classes (javax.management.remote.rmi) which are strongly encapsulated by the
     * module system. The `--add-opens` flags don't work reliably with ByteBuddy's bootstrap class
     * loader instrumentation in JDK 17+.
     *
     * @throws ClassNotFoundException    if reflection fails to find required classes
     * @throws NoSuchMethodException     if reflection fails to find required methods
     * @throws InvocationTargetException if method invocation fails during reflection
     * @throws IllegalAccessException    if reflection access is denied
     * @throws NoSuchFieldException      if reflection fails to find required fields
     */
    @Test
    @Timeout(value = 1, unit = TimeUnit.MINUTES)
    @DisabledOnJre(value = {JRE.JAVA_17, JRE.JAVA_18, JRE.JAVA_19, JRE.JAVA_20, JRE.JAVA_21, JRE.OTHER},
                   disabledReason = "Bytecode instrumentation of internal JMX classes is incompatible with JDK 17+ module system")
    void testJmxReconnect() throws Exception
    {
        // Validate JMX is working by calling ring API
        validateJmxWithRingAPI();

        // Retrieve the instances of RMIClientCommunicatorAdmin class.
        // We need to use reflection as it's loaded by the bootstrap classloader(null).
        Class<?> h = Class.forName("org.apache.cassandra.sidecar.health.JmxReconnectTest$ObjectHolder", false, null);
        List<Object> rmiClientCommunicatorAdminList = (List<Object>) h.getField("instance").get(null);

        // RMIClientCommunicatorAdmin is private inner class of RMIConnector.
        // Using reflection to update accessibility and to call gotIOException on a RMIClientCommunicatorAdmin object.
        Method gotIOExceptionMethod = rmiClientCommunicatorAdminList.get(1)
                                                                    .getClass()
                                                                    .getDeclaredMethod("gotIOException", IOException.class);
        gotIOExceptionMethod.setAccessible(true);
        gotIOExceptionMethod.invoke(rmiClientCommunicatorAdminList.get(1), new NoSuchObjectException("Injected"));

        // Validate JMX is working by calling ring API
        validateJmxWithRingAPI();
    }

    private void validateJmxWithRingAPI()
    {
        HttpResponse<Buffer> response = getBlocking(trustedClient()
                                                    .get(serverWrapper.serverPort, "localhost", ApiEndpointsV1.RING_ROUTE)
                                                    .send());
        assertThat(response.statusCode()).isEqualTo(200);

        RingResponse ringResponse = response.bodyAsJson(RingResponse.class);
        assertThat(ringResponse).isNotNull()
                                .isNotEmpty();
    }
}
