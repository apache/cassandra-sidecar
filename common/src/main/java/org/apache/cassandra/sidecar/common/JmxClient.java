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

import java.io.Closeable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.apache.cassandra.sidecar.common.exceptions.JmxAuthenticationException;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * A simple wrapper around a JMX connection that makes it easier to get proxy instances.
 */
public class JmxClient implements NotificationListener, Closeable
{
    public static final String JMX_PROTOCOL = "rmi";
    public static final String JMX_URL_PATH_FORMAT = "/jndi/rmi://%s:%d/jmxrmi";
    public static final String REGISTRY_CONTEXT_SOCKET_FACTORY = "com.sun.jndi.rmi.factory.socket";
    private final JMXServiceURL jmxServiceURL;
    private MBeanServerConnection mBeanServerConnection;
    private boolean connected = false;
    private JMXConnector jmxConnector;
    private final Supplier<String> roleSupplier;
    private final Supplier<String> passwordSupplier;
    private final BooleanSupplier enableSslSupplier;

    /**
     * Creates a new client with the provided {@code host} and {@code port}.
     *
     * @param host the host of the JMX service
     * @param port the port of the JMX service
     */
    public JmxClient(String host, int port)
    {
        this(host, port, null, null, false);
    }

    /**
     * Creates a new client with the provided parameters
     *
     * @param host      the host of the JMX service
     * @param port      the port of the JMX service
     * @param role      the JMX role used for authentication
     * @param password  the JMX role password used for authentication
     * @param enableSSl true if SSL is enabled for JMX, false otherwise
     */
    public JmxClient(String host, int port, String role, String password, boolean enableSSl)
    {
        this(buildJmxServiceURL(host, port), () -> role, () -> password, () -> enableSSl);
    }

    @VisibleForTesting
    JmxClient(JMXServiceURL jmxServiceURL)
    {
        this(jmxServiceURL, () -> null, () -> null, () -> false);
    }

    @VisibleForTesting
    JmxClient(JMXServiceURL jmxServiceURL, String role, String password)
    {
        this(jmxServiceURL, () -> role, () -> password, () -> false);
    }

    public JmxClient(String host,
                     int port,
                     Supplier<String> roleSupplier,
                     Supplier<String> passwordSupplier,
                     BooleanSupplier enableSslSupplier)
    {
        this(buildJmxServiceURL(host, port), roleSupplier, passwordSupplier, enableSslSupplier);
    }

    public JmxClient(JMXServiceURL jmxServiceURL,
                     Supplier<String> roleSupplier,
                     Supplier<String> passwordSupplier,
                     BooleanSupplier enableSslSupplier)
    {
        this.jmxServiceURL = Objects.requireNonNull(jmxServiceURL, "jmxServiceURL is required");
        this.roleSupplier = Objects.requireNonNull(roleSupplier, "roleSupplier is required");
        this.passwordSupplier = Objects.requireNonNull(passwordSupplier, "passwordSupplier is required");
        this.enableSslSupplier = Objects.requireNonNull(enableSslSupplier, "enableSslSupplier is required");
    }

    /**
     * Returns a proxy for a Standard MBean in a local or remote MBean Server.
     *
     * @param clientClass the management interface that the MBean exports, which will
     *                    also be implemented by the returned proxy
     * @param remoteName  the name of the MBean within {@code connection} to forward to
     * @param <C>         the type of the proxy client
     * @return the proxy for a Standard MBean in a local or remote MBean Server
     */
    public <C> C proxy(Class<C> clientClass, String remoteName)
    {
        checkConnection();
        try
        {
            // NOTE: We get a new proxy each time we need one (much of the underlying construction is
            // cached by the JMX infrastructure, so we believe this to not be terribly resource-intensive
            ObjectName name = new ObjectName(remoteName);
            return JMX.newMBeanProxy(mBeanServerConnection, name, clientClass);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(String.format("Invalid remote object name '%s'", remoteName), e);
        }
    }

    private RMIClientSocketFactory rmiClientSocketFactory(boolean enableSsl)
    {
        return enableSsl
               ? new SslRMIClientSocketFactory()
               : RMISocketFactory.getDefaultSocketFactory();
    }

    private synchronized void checkConnection()
    {
        if (!this.connected)
        {
            connect();
        }
    }

    private void connect()
    {
        try
        {
            jmxConnector = JMXConnectorFactory.connect(jmxServiceURL, buildJmxEnv());
            jmxConnector.addConnectionNotificationListener(this, null, null);
            mBeanServerConnection = jmxConnector.getMBeanServerConnection();
            connected = true;
        }
        catch (IOException iox)
        {
            connected = false;
            throw new RuntimeException(String.format("Failed to connect to JMX endpoint %s", jmxServiceURL),
                                       iox);
        }
        catch (SecurityException securityException)
        {
            connected = false;
            String errorMessage = securityException.getMessage() != null
                                  ? securityException.getMessage()
                                  : "JMX Authentication failed";
            throw new JmxAuthenticationException(errorMessage, securityException);
        }
        catch (RuntimeException runtimeException)
        {
            // catch exceptions coming from the lambdas and wrap them in a JmxAuthenticationException
            throw new JmxAuthenticationException(runtimeException);
        }
    }

    @Override
    public void handleNotification(Notification notification, Object handback)
    {
        if (notification instanceof JMXConnectionNotification)
        {
            JMXConnectionNotification connectNotice = (JMXConnectionNotification) notification;
            final String type = connectNotice.getType();
            if (type.equals(JMXConnectionNotification.CLOSED) ||
                type.equals(JMXConnectionNotification.FAILED) ||
                type.equals(JMXConnectionNotification.NOTIFS_LOST) ||
                type.equals(JMXConnectionNotification.OPENED))
            {
                boolean justConnected = type.equals(JMXConnectionNotification.OPENED);
                synchronized (this)
                {
                    this.connected = justConnected;
                }
            }
        }
    }

    /**
     * @return true if JMX is connected, false otherwise
     */
    public boolean isConnected()
    {
        return connected;
    }

    public String host()
    {
        return jmxServiceURL.getHost();
    }

    public int port()
    {
        return jmxServiceURL.getPort();
    }

    private static JMXServiceURL buildJmxServiceURL(String host, int port)
    {
        if (host == null)
            return null;

        try
        {
            return new JMXServiceURL(JMX_PROTOCOL, host, port, jmxUrlPath(host, port));
        }
        catch (MalformedURLException e)
        {
            String errorMessage = String.format("Unable to build JMXServiceURL for host=%s, port=%d",
                                                host, port);
            throw new RuntimeException(errorMessage, e);
        }
    }

    private Map<String, Object> buildJmxEnv()
    {
        String role = roleSupplier.get();
        String password = passwordSupplier.get();
        boolean enableSsl = enableSslSupplier.getAsBoolean();

        Map<String, Object> jmxEnv = new HashMap<>();
        if (role != null && password != null)
        {
            String[] credentials = new String[]{ role, password };
            jmxEnv.put(JMXConnector.CREDENTIALS, credentials);
        }
        jmxEnv.put(REGISTRY_CONTEXT_SOCKET_FACTORY, rmiClientSocketFactory(enableSsl));
        return jmxEnv;
    }

    @Override
    public synchronized void close() throws IOException
    {
        JMXConnector connector = jmxConnector;
        if (connector != null)
        {
            jmxConnector = null;
            connector.close();
        }
    }

    private static String jmxUrlPath(String host, int port)
    {
        return String.format(JMX_URL_PATH_FORMAT, maybeAddSquareBrackets(host), port);
    }

    private static String maybeAddSquareBrackets(String host)
    {
        if (host == null // host is null
            || host.isEmpty() // or host is empty
            || host.charAt(0) == '[' // host already starts with square brackets
            || !host.contains(":")) // or host doesn't contain ":" (not an IPv6 address)
            return host;

        // Use square brackets to surround IPv6 addresses to fix CASSANDRA-7669 and CASSANDRA-17581
        return "[" + host + "]";
    }
}
