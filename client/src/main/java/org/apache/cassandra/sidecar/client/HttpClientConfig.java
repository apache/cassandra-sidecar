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

package org.apache.cassandra.sidecar.client;

import java.io.InputStream;

/**
 * Encapsulates {@code HttpClient} configuration parameters.
 */
public class HttpClientConfig
{
    public static final long DEFAULT_TIMEOUT_MILLIS = 30_000;
    public static final boolean DEFAULT_SSL = true;
    public static final int DEFAULT_MAX_POOL_SIZE = 20;
    public static final String DEFAULT_USER_AGENT = "sidecar-client/1.0.0";
    public static final int DEFAULT_IDLE_TIMEOUT_MILLIS = 0; // no timeout
    public static final int DEFAULT_MAX_CHUNK_SIZE = 6 * 1024 * 1024; // 6 MiB
    public static final int DEFAULT_RECEIVE_BUFFER_SIZE = -1;
    public static final int DEFAULT_READ_BUFFER_SIZE = 8 * 1024; // 8 KiB
    public static final String DEFAULT_TRUST_STORE_TYPE = "JKS";
    public static final String DEFAULT_KEY_STORE_TYPE = "PKCS12";

    private final long timeoutMillis;
    private final boolean ssl;
    private final int maxPoolSize;
    private final String userAgent;
    private final int idleTimeoutMillis;
    private final int maxChunkSize;
    private final int receiveBufferSize;
    private final int readBufferSize;
    private final InputStream trustStoreInputStream;
    private final String trustStorePassword;
    private final String trustStoreType;
    private final InputStream keyStoreInputStream;
    private final String keyStorePassword;
    private final String keyStoreType;

    private HttpClientConfig(Builder<?> builder)
    {
        timeoutMillis = builder.timeoutMillis;
        ssl = builder.ssl;
        maxPoolSize = builder.maxPoolSize;
        userAgent = builder.userAgent;
        idleTimeoutMillis = builder.idleTimeoutMillis;
        maxChunkSize = builder.maxChunkSize;
        receiveBufferSize = builder.receiveBufferSize;
        readBufferSize = builder.readBufferSize;
        trustStoreInputStream = builder.trustStoreInputStream;
        trustStorePassword = builder.trustStorePassword;
        trustStoreType = builder.trustStoreType;
        keyStoreInputStream = builder.keyStoreInputStream;
        keyStorePassword = builder.keyStorePassword;
        keyStoreType = builder.keyStoreType;
    }

    /**
     * @return the amount of time in milliseconds to wait to receive any data before failing the request
     */
    public long timeoutMillis()
    {
        return timeoutMillis;
    }

    /**
     * @return true if SSL is used, false otherwise
     */
    public boolean ssl()
    {
        return ssl;
    }

    /**
     * @return the maximum pool size for connections
     */
    public int maxPoolSize()
    {
        return maxPoolSize;
    }

    /**
     * @return the user agent to use for the HTTP client
     */
    public String userAgent()
    {
        return userAgent;
    }

    /**
     * @return the amount of time to wait for data to be sent or received before the connection is timed out and closed
     */
    public int idleTimeoutMillis()
    {
        return idleTimeoutMillis;
    }

    /**
     * @return the maximum HTTP chunk size
     */
    public int maxChunkSize()
    {
        return maxChunkSize;
    }

    /**
     * @return the TCP receive buffer size
     */
    public int receiveBufferSize()
    {
        return receiveBufferSize;
    }

    public int sendReadBufferSize()
    {
        return readBufferSize;
    }

    /**
     * @return the {@link InputStream} for the truststore to use for the request trust options
     */
    public InputStream trustStoreInputStream()
    {
        return trustStoreInputStream;
    }

    /**
     * @return the password for the provided {@link #trustStorePassword}
     */
    public String trustStorePassword()
    {
        return trustStorePassword;
    }

    /**
     * @return the trust store type
     */
    public String trustStoreType()
    {
        return trustStoreType;
    }

    /**
     * @return the {@link InputStream} for the keystore to use for the request key cert options
     */
    public InputStream keyStoreInputStream()
    {
        return keyStoreInputStream;
    }

    /**
     * @return the password for the provided {@link #keyStorePassword}
     */
    public String keyStorePassword()
    {
        return keyStorePassword;
    }

    /**
     * @return the keystore type
     */
    public String keyStoreType()
    {
        return keyStoreType;
    }

    /**
     * {@code HttpClient} builder static inner class.
     *
     * @param <T> the type of the Builder
     */
    public static class Builder<T extends Builder<T>>
    {
        private long timeoutMillis = DEFAULT_TIMEOUT_MILLIS;
        private boolean ssl = DEFAULT_SSL;
        private int maxPoolSize = DEFAULT_MAX_POOL_SIZE;
        public int idleTimeoutMillis = DEFAULT_IDLE_TIMEOUT_MILLIS;
        private String userAgent = DEFAULT_USER_AGENT;
        private int maxChunkSize = DEFAULT_MAX_CHUNK_SIZE;
        private int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
        private int readBufferSize = DEFAULT_READ_BUFFER_SIZE;
        private InputStream trustStoreInputStream;
        private String trustStorePassword;
        private String trustStoreType = DEFAULT_TRUST_STORE_TYPE;
        private InputStream keyStoreInputStream;
        private String keyStorePassword;
        private String keyStoreType = DEFAULT_KEY_STORE_TYPE;

        /**
         * @return a reference to itself
         */
        @SuppressWarnings("unchecked")
        protected T self()
        {
            return (T) this;
        }

        /**
         * Sets the {@code timeoutMillis} and returns a reference to this Builder enabling method chaining.
         * Defaults to {@code 30,000} milliseconds ({@code 30} seconds).
         *
         * @param timeoutMillis the {@code timeoutMillis} to set
         * @return a reference to this Builder
         */
        public T timeoutMillis(long timeoutMillis)
        {
            this.timeoutMillis = timeoutMillis;
            return self();
        }

        /**
         * Sets the {@code ssl} and returns a reference to this Builder enabling method chaining. Defaults to
         * {@code false}.
         *
         * @param ssl the {@code ssl} to set
         * @return a reference to this Builder
         */
        public T ssl(boolean ssl)
        {
            this.ssl = ssl;
            return self();
        }

        /**
         * Sets the maximum pool size for connections and returns a reference to this Builder enabling method chaining.
         * Defaults to {@code 20}.
         *
         * @param maxPoolSize the maximum pool size
         * @return a reference to this Builder
         */
        public T maxPoolSize(int maxPoolSize)
        {
            this.maxPoolSize = maxPoolSize;
            return self();
        }

        /**
         * Set the Web Client {@code userAgent} and returns a reference to this Builder enabling method chaining.
         * Defaults to sidecar-client/1.0.0
         *
         * @param userAgent the {@code userAgent} to set
         * @return a reference to this Builder
         */
        public T userAgent(String userAgent)
        {
            this.userAgent = userAgent;
            return self();
        }

        /**
         * Sets the {@code idleTimeoutMillis}. Zero means do not timeout. This determines if a connection will timeout
         * and be closed if no data is received nor sent within the timeout.
         *
         * @param idleTimeoutMillis to {@code idleTimeoutMillis} to set
         * @return a reference to this Builder
         */
        public T idleTimeoutMillis(int idleTimeoutMillis)
        {
            this.idleTimeoutMillis = idleTimeoutMillis;
            return self();
        }

        /**
         * Set the maximum HTTP chunk size {@code maxChunkSize} and returns a reference to this Builder enabling
         * method chaining. Defaults to {@code 6 MiB}.
         *
         * @param maxChunkSize the {@code maxChunkSize} to set
         * @return a reference to this Builder
         */
        public T maxChunkSize(int maxChunkSize)
        {
            this.maxChunkSize = maxChunkSize;
            return self();
        }

        /**
         * Sets the {@code receiveBufferSize} and returns a reference to this Builder enabling method chaining.
         * The default value of TCP receive buffer size is -1.
         *
         * @param receiveBufferSize the {@code receiveBufferSize} to set
         * @return a reference to this Builder
         */
        public T receiveBufferSize(int receiveBufferSize)
        {
            this.receiveBufferSize = receiveBufferSize;
            return self();
        }

        /**
         * Sets the {@code readBufferSize} that will be used to read the data from the files for upload.
         * Changing this value will impact how much the data will be read at a time from the file system.
         *
         * @param readBufferSize the buffer size
         * @return a reference to this Builder
         */
        public T readBufferSize(int readBufferSize)
        {
            this.readBufferSize = readBufferSize;
            return self();
        }

        /**
         * Sets the {@code trustStoreInputStream} and returns a reference to this Builder enabling method chaining.
         *
         * @param trustStoreInputStream the {@code trustStoreInputStream} to set
         * @return a reference to this Builder
         */
        public T trustStoreInputStream(InputStream trustStoreInputStream)
        {
            this.trustStoreInputStream = trustStoreInputStream;
            return self();
        }

        /**
         * Sets the {@code trustStorePassword} and returns a reference to this Builder enabling method chaining.
         *
         * @param trustStorePassword the {@code trustStorePassword} to set
         * @return a reference to this Builder
         */
        public T trustStorePassword(String trustStorePassword)
        {
            this.trustStorePassword = trustStorePassword;
            return self();
        }

        /**
         * Sets the {@code trustStoreType} and returns a reference to this Builder enabling method chaining.
         *
         * @param trustStoreType the {@code trustStoreType} to set
         * @return a reference to this Builder
         */
        public T trustStoreType(String trustStoreType)
        {
            this.trustStoreType = trustStoreType;
            return self();
        }

        /**
         * Sets the {@code keyStoreInputStream} and returns a reference to this Builder enabling method chaining.
         *
         * @param keyStoreInputStream the {@code keyStoreInputStream} to set
         * @return a reference to this Builder
         */
        public T keyStoreInputStream(InputStream keyStoreInputStream)
        {
            this.keyStoreInputStream = keyStoreInputStream;
            return self();
        }

        /**
         * Sets the {@code keyStorePassword} and returns a reference to this Builder enabling method chaining.
         *
         * @param keyStorePassword the {@code keyStorePassword} to set
         * @return a reference to this Builder
         */
        public T keyStorePassword(String keyStorePassword)
        {
            this.keyStorePassword = keyStorePassword;
            return self();
        }

        /**
         * Sets the {@code keyStoreType} and returns a reference to this Builder enabling method chaining.
         *
         * @param keyStoreType the {@code keyStoreType} to set
         * @return a reference to this Builder
         */
        public T keyStoreType(String keyStoreType)
        {
            this.keyStoreType = keyStoreType;
            return self();
        }

        /**
         * Returns a {@code SidecarClientConfig} built from the parameters previously set.
         *
         * @return a {@code SidecarClientConfig} built with parameters of this {@code SidecarClientConfig.Builder}
         */
        public HttpClientConfig build()
        {
            return new HttpClientConfig(this);
        }
    }
}
