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

package org.apache.cassandra.sidecar.config.yaml;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.http.HttpMethod;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SchemaReportingConfiguration;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration for Schema Reporting
 */
public class SchemaReportingConfigurationImpl extends PeriodicTaskConfigurationImpl implements SchemaReportingConfiguration
{
    protected static final boolean DEFAULT_ENABLED = false;
    protected static final MillisecondBoundConfiguration DEFAULT_INITIAL_DELAY = new MillisecondBoundConfiguration(6L, TimeUnit.HOURS);
    protected static final MillisecondBoundConfiguration DEFAULT_EXECUTE_INTERVAL = new MillisecondBoundConfiguration(12L, TimeUnit.HOURS);
    protected static final String DEFAULT_ENDPOINT = null;
    protected static final String DEFAULT_METHOD = HttpMethod.PUT.name();
    protected static final int DEFAULT_MAX_RETRIES = 3;
    protected static final MillisecondBoundConfiguration DEFAULT_RETRY_DELAY = new MillisecondBoundConfiguration(1L, TimeUnit.MINUTES);

    @JsonProperty(value = "endpoint")
    @Nullable
    protected final String endpoint;
    @JsonProperty(value = "method")
    @NotNull
    protected final String method;
    @JsonProperty(value = "max_retries")
    protected final int maxRetries;
    @JsonProperty(value = "retry_delay")
    @NotNull
    protected final MillisecondBoundConfiguration retryDelay;

    /**
     * Constructs an instance of {@link SchemaReportingConfigurationImpl} with default settings
     */
    public SchemaReportingConfigurationImpl()
    {
        this(DEFAULT_ENABLED,
             DEFAULT_INITIAL_DELAY,
             DEFAULT_EXECUTE_INTERVAL,
             DEFAULT_ENDPOINT,
             DEFAULT_METHOD,
             DEFAULT_MAX_RETRIES,
             DEFAULT_RETRY_DELAY);
    }

    /**
     * Constructs an instance of {@link SchemaReportingConfigurationImpl} with custom settings
     *
     * @param enabled whether to report cluster schemata; {@code false} by default
     * @param initialDelay maximum delay before the initial schema report used to prevent the thundering herd problem
     *                     (the actual delay will be randomized for each execution, but will not exceed this value);
     *                     6 hours by default
     * @param executeInterval exact interval between two consecutive reports of the same schema; 12 hours by default
     * @param endpoint endpoint address for schema reporting; empty by default
     * @param method HTTP verb to use when reporting schemata; {@code PUT} by default
     * @param maxRetries number of times a schema report is retried in case of failure; {@code 3} by default
     * @param retryDelay delay before a schema report is retried in case of failure; one minute by default
     */
    public SchemaReportingConfigurationImpl(boolean enabled,
                                            @NotNull MillisecondBoundConfiguration initialDelay,
                                            @NotNull MillisecondBoundConfiguration executeInterval,
                                            @Nullable String endpoint,
                                            @NotNull String method,
                                            int maxRetries,
                                            @NotNull MillisecondBoundConfiguration retryDelay)
    {
        super(enabled,
              initialDelay,
              executeInterval);

        this.endpoint = endpoint;
        this.method = method;
        this.maxRetries = maxRetries;
        this.retryDelay = retryDelay;
    }

    /**
     * An endpoint address for schema reporting; empty by default
     */
    @Override
    @Nullable
    public String endpoint()
    {
        return endpoint;
    }

    /**
     * An HTTP verb to use when reporting schemata; {@code PUT} by default
     */
    @Override
    @NotNull
    public HttpMethod method()
    {
        return HttpMethod.valueOf(method);
    }

    /**
     * A number of times a schema report is retried in case of failure; {@code 3} by default
     */
    @Override
    public int maxRetries()
    {
        return maxRetries;
    }

    /**
     * A delay before a schema report is retried in case of failure; one minute by default
     */
    @Override
    @NotNull
    public MillisecondBoundConfiguration retryDelay()
    {
        return retryDelay;
    }
}
