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
import java.net.MalformedURLException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMISocketFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
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

import org.jetbrains.annotations.VisibleForTesting;

/**
 * A simple wrapper around a JMX connection that makes it easier to get proxy instances.
 */
public class JmxClient implements NotificationListener
{
    public static final String JMX_SERVICE_URL_FMT = "service:jmx:rmi://%s/jndi/rmi://%s:%d/jmxrmi";
    public static final String REGISTRY_CONTEXT_SOCKET_FACTORY = "com.sun.jndi.rmi.factory.socket";
    private final JMXServiceURL jmxServiceURL;
    private MBeanServerConnection mBeanServerConnection;
    private final Map<String, Object> jmxEnv;
    private boolean connected = false;

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
        this(buildJmxServiceURL(host, port), role, password, enableSSl);
    }

    @VisibleForTesting
    JmxClient(JMXServiceURL jmxServiceURL)
    {
        this(jmxServiceURL, null, null, false);
    }

    @VisibleForTesting
    JmxClient(JMXServiceURL jmxServiceURL, String role, String password)
    {
        this(jmxServiceURL, role, password, false);
    }

    private JmxClient(JMXServiceURL jmxServiceURL, String role, String password, boolean enableSsl)
    {
        this.jmxServiceURL = jmxServiceURL;

        jmxEnv = new HashMap<>();
        if (role != null && password != null)
        {
            String[] credentials = new String[]{ role, password };
            jmxEnv.put(JMXConnector.CREDENTIALS, credentials);
        }
        jmxEnv.put(REGISTRY_CONTEXT_SOCKET_FACTORY, getRMIClientSocketFactory(enableSsl));
    }

    /**
     * Applies a function through the JMX bean proxy
     *
     * @param clientClass the management interface that the MBean exports, which will
     *                    also be implemented by the returned proxy
     * @param remoteName  the name of the MBean within {@code connection} to forward to
     * @param func        the remote function that will be executed through the proxy
     * @param <C>         the type of the proxy client
     * @param <R>         the type of the result returned by the proxy
     * @return the result of the remote function after being executed via the proxy
     */
    public <C, R> R apply(Class<C> clientClass, String remoteName, Function<C, R> func)
    {
        checkConnection();
        C client = getProxy(clientClass, remoteName);
        return func.apply(client);
    }

    /**
     * Calls a remote function through the JMX bean proxy
     *
     * @param clientClass the management interface that the MBean exports, which will
     *                    also be implemented by the returned proxy
     * @param remoteName  the name of the MBean within {@code connection} to forward to
     * @param caller      the consumer to be called through the proxy
     * @param <C>         the type of the proxy client
     */
    public <C> void call(Class<C> clientClass, String remoteName, Consumer<C> caller)
    {
        checkConnection();
        C client = getProxy(clientClass, remoteName);
        caller.accept(client);
    }

    private <C> C getProxy(Class<C> clientClass, String remoteName)
    {
        try
        {
            // NOTE: Because proxy instances can "go stale" if the connection drops,
            // we don't allow consumers to hold references to them.  Instead, we get a new proxy
            // each time we need one (much of the underlying construction is cached by the JMX infrastructure
            // so we believe this to not be terribly resource-intensive
            ObjectName name = new ObjectName(remoteName);
            return JMX.newMBeanProxy(mBeanServerConnection, name, clientClass);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(String.format("Invalid remote object name '%s'", remoteName), e);
        }
    }

    private RMIClientSocketFactory getRMIClientSocketFactory(boolean enableSsl)
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
            JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxServiceURL, jmxEnv);
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
    }

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

    private static JMXServiceURL buildJmxServiceURL(String host, int port)
    {
        if (host == null)
            return null;

        if (host.contains(":"))
        {
            host = "[" + host + "]";
            // Use square brackets to surround IPv6 addresses to fix CASSANDRA-7669 and CASSANDRA-17581
        }
        try
        {
            return new JMXServiceURL(String.format(JMX_SERVICE_URL_FMT, host, host, port));
        }
        catch (MalformedURLException e)
        {
            String errorMessage = String.format("Unable to build JMXServiceURL for host=%s, port=%d",
                                                host, port);
            throw new RuntimeException(errorMessage, e);
        }
    }
}
