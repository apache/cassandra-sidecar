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
import org.apache.cassandra.sidecar.config.CoordinationConfiguration;

/**
 * Configuration relevant to the coordination functionality of Sidecar
 */
public class CoordinationConfigurationImpl implements CoordinationConfiguration
{
    public static final boolean DEFAULT_SINGLE_INSTANCE_EXECUTOR_PROCESS_ENABLED = true;
    public static final long DEFAULT_SINGLE_INSTANCE_EXECUTOR_FREQUENCY_MILLIS = TimeUnit.MINUTES.toMillis(1);
    public static final long DEFAULT_SINGLE_INSTANCE_EXECUTOR_INITIAL_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(1);

    @JsonProperty("single_instance_executor_process_enabled")
    public boolean singleInstanceExecutorProcessEnabled;
    @JsonProperty("single_instance_executor_frequency_millis")
    public long singleInstanceExecutorFrequencyMillis;
    @JsonProperty("single_instance_executor_initial_delay_millis")
    public long singleInstanceExecutorInitialDelayMillis;

    public CoordinationConfigurationImpl()
    {
        this(DEFAULT_SINGLE_INSTANCE_EXECUTOR_PROCESS_ENABLED,
             DEFAULT_SINGLE_INSTANCE_EXECUTOR_INITIAL_DELAY_MILLIS,
             DEFAULT_SINGLE_INSTANCE_EXECUTOR_FREQUENCY_MILLIS);
    }

    public CoordinationConfigurationImpl(boolean singleInstanceExecutorProcessEnabled,
                                         long singleInstanceExecutorInitialDelayMillis,
                                         long singleInstanceExecutorFrequencyMillis)
    {
        this.singleInstanceExecutorProcessEnabled = singleInstanceExecutorProcessEnabled;
        this.singleInstanceExecutorInitialDelayMillis = singleInstanceExecutorInitialDelayMillis;
        this.singleInstanceExecutorFrequencyMillis = singleInstanceExecutorFrequencyMillis;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("single_instance_executor_process_enabled")
    public boolean singleInstanceExecutorProcessEnabled()
    {
        return singleInstanceExecutorProcessEnabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("single_instance_executor_frequency_millis")
    public long singleInstanceExecutorFrequencyMillis()
    {
        return singleInstanceExecutorFrequencyMillis;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("single_instance_executor_initial_delay_millis")
    public long singleInstanceExecutorInitialDelayMillis()
    {
        return singleInstanceExecutorInitialDelayMillis;
    }
}
