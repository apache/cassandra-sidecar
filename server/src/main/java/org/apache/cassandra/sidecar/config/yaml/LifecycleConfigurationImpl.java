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

import java.util.Collections;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.LifecycleConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.lifecycle.ProcessLifecycleProvider;

/**
 * Configuration for Cassandra lifecycle management service
 */
public class LifecycleConfigurationImpl implements LifecycleConfiguration
{
    private static final boolean DEFAULT_ENABLED = false;
    private static final ParameterizedClassConfiguration DEFAULT_LIFECYCLE_PROVIDER =
            new ParameterizedClassConfigurationImpl(ProcessLifecycleProvider.class.getName(), Collections.emptyMap());

    protected final Boolean enabled;

    protected final String directory;

    protected final ParameterizedClassConfiguration lifecycleProvider;

    public LifecycleConfigurationImpl()
    {
        this(DEFAULT_ENABLED, null, DEFAULT_LIFECYCLE_PROVIDER);
    }

    @JsonCreator
    public LifecycleConfigurationImpl(@JsonProperty("enabled") Boolean enabled,
                                      @JsonProperty("directory") String directory,
                                      @JsonProperty("provider") ParameterizedClassConfiguration lifecycleProvider)
    {
        this.enabled = enabled;
        this.directory = directory;
        this.lifecycleProvider = lifecycleProvider;
    }

    @JsonProperty(value = "enabled")
    public boolean enabled()
    {
        return enabled;
    }

    @JsonProperty(value = "provider")
    public ParameterizedClassConfiguration lifecycleProvider()
    {
        return lifecycleProvider;
    }
}
