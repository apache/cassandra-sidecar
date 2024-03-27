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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.MetricsFilteringConfiguration;

/**
 * Holds configuration needed for filtering out metrics
 */
public class MetricsFilteringConfigurationImpl implements MetricsFilteringConfiguration
{
    public static final String DEFAULT_TYPE = "regex";
    public static final String DEFAULT_PATTERN = ".*"; // include all metrics
    public static final boolean DEFAULT_INVERSE = false;
    @JsonProperty("type")
    private final String type;
    @JsonProperty("pattern")
    private final String pattern;
    @JsonProperty("inverse")
    private final boolean inverse;

    public MetricsFilteringConfigurationImpl()
    {
        this(DEFAULT_TYPE, DEFAULT_PATTERN, DEFAULT_INVERSE);
    }

    public MetricsFilteringConfigurationImpl(String type, String pattern, boolean inverse)
    {
        this.type = type;
        this.pattern = pattern;
        this.inverse = inverse;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String type()
    {
        return type;
    }

    /**
     * {@inheritDoc}
     */
    public String pattern()
    {
        return pattern;
    }

    /**
     * {@inheritDoc}
     */
    public boolean inverse()
    {
        return inverse;
    }
}
