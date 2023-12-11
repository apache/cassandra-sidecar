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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.exceptions.JmxAuthenticationException;
import org.apache.cassandra.sidecar.common.utils.Preconditions;

/**
 * A simple wrapper around a JMX connection that makes it easier to get proxy instances.
 */
public class JmxClient implements NotificationListener, Closeable
{
    public static final String JMX_PROTOCOL = "rmi";
    public static final String JMX_URL_PATH_FORMAT = "/jndi/rmi://%s:%d/jmxrmi";
    public static final String REGISTRY_CONTEXT_SOCKET_FACTORY = "com.sun.jndi.rmi.factory.socket";
    private static final Logger LOGGER = LoggerFactory.getLogger(JmxClient.class);
    private final JMXServiceURL jmxServiceURL;
    private MBeanServerConnection mBeanServerConnection;
    private boolean connected = false;
    private JMXConnector jmxConnector;
    private final Supplier<String> roleSupplier;
    private final Supplier<String> passwordSupplier;
    private final BooleanSupplier enableSslSupplier;
    private final int connectionMaxRetries;
    private final long connectionRetryDelayMillis;
    private final Set<NotificationListener> registeredNotificationListeners =
    Collections.newSetFromMap(new ConcurrentHashMap<>());

    /**
     * Creates a new JMX client with {@link Builder} options.
     *
     * @param builder the builder options
     */
    private JmxClient(Builder builder)
    {
        if (builder.jmxServiceURL != null)
        {
            jmxServiceURL = builder.jmxServiceURL;
        }
        else
        {
            jmxServiceURL = buildJmxServiceURL(Objects.requireNonNull(builder.host, "host is required"),
                                               builder.port);
        }
        Objects.requireNonNull(jmxServiceURL, "jmxServiceUrl is required");
        roleSupplier = Objects.requireNonNull(builder.roleSupplier, "roleSupplier is required");
        passwordSupplier = Objects.requireNonNull(builder.passwordSupplier, "passwordSupplier is required");
        enableSslSupplier = Objects.requireNonNull(builder.enableSslSupplier, "enableSslSupplier is required");
        Preconditions.checkArgument(builder.connectionMaxRetries > 0,
                                    "connectionMaxRetries must be a positive integer");
        connectionMaxRetries = builder.connectionMaxRetries;
        connectionRetryDelayMillis = builder.connectionRetryDelayMillis;
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

    /**
     * Registers a {@link NotificationListener} to be notified whenever we encounter a JMX event. This method
     * guarantees that a listener will be registered at most once.
     *
     * @param notificationListener the listener to be notified
     */
    public void registerListener(NotificationListener notificationListener)
    {
        registeredNotificationListeners.add(notificationListener);
    }

    /**
     * Removes an already registered {@link NotificationListener} from the recipient list for JMX events.
     *
     * @param notificationListener the listener to be removed
     */
    public void unregisterListener(NotificationListener notificationListener)
    {
        registeredNotificationListeners.remove(notificationListener);
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
        int attempts = 1;
        int maxAttempts = connectionMaxRetries;
        Throwable lastThrown = null;
        while (attempts <= maxAttempts)
        {
            try
            {
                connectInternal(attempts);
                return;
            }
            // Unrecoverable errors
            catch (SecurityException securityException)
            {
                // If we can't connect because we have bad credentials, don't retry
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
            // Anything else is recoverable so we should retry.
            catch (Throwable t)
            {
                lastThrown = t;
                if (attempts < maxAttempts)
                {
                    LOGGER.info("Could not connect to JMX on {} after {} attempts. Will retry.",
                                jmxServiceURL, attempts, t);
                    Uninterruptibles.sleepUninterruptibly(connectionRetryDelayMillis, TimeUnit.MILLISECONDS);
                }
                attempts++;
            }
        }
        String error = "Failed to connect to JMX, which was unreachable after " + attempts + " attempts.";
        LOGGER.error(error, lastThrown);
        throw new RuntimeException(error, lastThrown);
    }

    private void connectInternal(int currentAttempt) throws IOException
    {
        jmxConnector = JMXConnectorFactory.connect(jmxServiceURL, buildJmxEnv());
        jmxConnector.addConnectionNotificationListener(this, null, null);
        mBeanServerConnection = jmxConnector.getMBeanServerConnection();
        connected = true;
        LOGGER.info("Connected to JMX server at {} after {} attempt(s)",
                    jmxServiceURL, currentAttempt);
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
                forwardNotification(notification, handback);
            }
        }
    }

    private void forwardNotification(Notification notification, Object handback)
    {
        registeredNotificationListeners.forEach(listener -> listener.handleNotification(notification, handback));
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
    public void close() throws IOException
    {
        JMXConnector connector;
        synchronized (this)
        {
            connector = jmxConnector;
            jmxConnector = null;
            connected = false;
        }
        if (connector != null)
        {
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

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code JmxClient} builder static inner class.
     */
    public static final class Builder implements DataObjectBuilder<Builder, JmxClient>
    {
        private JMXServiceURL jmxServiceURL;
        private String host;
        private int port;
        private Supplier<String> roleSupplier = () -> null;
        private Supplier<String> passwordSupplier = () -> null;
        private BooleanSupplier enableSslSupplier = () -> false;
        private int connectionMaxRetries = 3;
        private long connectionRetryDelayMillis = 200;

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code host} and returns a reference to this Builder enabling method chaining.
         *
         * @param host the {@code host} to set
         * @return a reference to this Builder
         */
        public Builder host(String host)
        {
            return update(b -> b.host = host);
        }

        /**
         * Sets the {@code port} and returns a reference to this Builder enabling method chaining.
         *
         * @param port the {@code port} to set
         * @return a reference to this Builder
         */
        public Builder port(int port)
        {
            return update(b -> b.port = port);
        }

        /**
         * Sets the {@code jmxServiceURL} and returns a reference to this Builder enabling method chaining.
         *
         * @param jmxServiceURL the {@code jmxServiceURL} to set
         * @return a reference to this Builder
         */
        public Builder jmxServiceURL(JMXServiceURL jmxServiceURL)
        {
            return update(b -> b.jmxServiceURL = jmxServiceURL);
        }

        /**
         * Sets the {@code roleSupplier} and returns a reference to this Builder enabling method chaining.
         *
         * @param roleSupplier the {@code roleSupplier} to set
         * @return a reference to this Builder
         */
        public Builder roleSupplier(Supplier<String> roleSupplier)
        {
            return update(b -> b.roleSupplier = Objects.requireNonNull(roleSupplier,
                                                                       "roleSupplier must be provided"));
        }

        /**
         * Sets the {@code roleSupplier} and returns a reference to this Builder enabling method chaining.
         *
         * @param role the {@code role} to set
         * @return a reference to this Builder
         */
        public Builder role(String role)
        {
            return update(b -> b.roleSupplier = () -> role);
        }

        /**
         * Sets the {@code passwordSupplier} and returns a reference to this Builder enabling method chaining.
         *
         * @param passwordSupplier the {@code passwordSupplier} to set
         * @return a reference to this Builder
         */
        public Builder passwordSupplier(Supplier<String> passwordSupplier)
        {
            return update(b -> b.passwordSupplier = Objects.requireNonNull(passwordSupplier,
                                                                           "passwordSupplier must be provided"));
        }

        /**
         * Sets the {@code passwordSupplier} and returns a reference to this Builder enabling method chaining.
         *
         * @param password the {@code password} to set
         * @return a reference to this Builder
         */
        public Builder password(String password)
        {
            return update(b -> b.passwordSupplier = () -> password);
        }

        /**
         * Sets the {@code enableSslSupplier} and returns a reference to this Builder enabling method chaining.
         *
         * @param enableSslSupplier the {@code enableSslSupplier} to set
         * @return a reference to this Builder
         */
        public Builder enableSslSupplier(BooleanSupplier enableSslSupplier)
        {
            return update(b -> b.enableSslSupplier = enableSslSupplier);
        }

        /**
         * Sets the {@code enableSslSupplier} and returns a reference to this Builder enabling method chaining.
         *
         * @param enableSsl the {@code enableSsl} to set
         * @return a reference to this Builder
         */
        public Builder enableSsl(boolean enableSsl)
        {
            return update(b -> b.enableSslSupplier = () -> enableSsl);
        }

        /**
         * Sets the {@code connectionMaxRetries} and returns a reference to this Builder enabling method chaining.
         *
         * @param connectionMaxRetries the {@code connectionMaxRetries} to set
         * @return a reference to this Builder
         */
        public Builder connectionMaxRetries(int connectionMaxRetries)
        {
            return update(b -> b.connectionMaxRetries = connectionMaxRetries);
        }

        /**
         * Sets the {@code connectionRetryDelayMillis} and returns a reference to this Builder enabling method chaining.
         *
         * @param connectionRetryDelayMillis the {@code connectionRetryDelayMillis} to set
         * @return a reference to this Builder
         */
        public Builder connectionRetryDelayMillis(long connectionRetryDelayMillis)
        {
            return update(b -> b.connectionRetryDelayMillis = connectionRetryDelayMillis);
        }

        /**
         * Returns a {@code JmxClient} built from the parameters previously set.
         *
         * @return a {@code JmxClient} built with parameters of this {@code JmxClient.Builder}
         */
        @Override
        public JmxClient build()
        {
            return new JmxClient(this);
        }
    }
}
