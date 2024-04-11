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
    public static final String REGEX_TYPE = "regex";
    public static final String EQUALS_TYPE = "equals";
    public static final String DEFAULT_TYPE = REGEX_TYPE;
    public static final String DEFAULT_VALUE = ".*"; // include all metrics
    private String type;
    @JsonProperty("value")
    private final String value;

    public MetricsFilteringConfigurationImpl()
    {
        this(DEFAULT_TYPE, DEFAULT_VALUE);
    }

    public MetricsFilteringConfigurationImpl(String type, String value)
    {
        this.type = type;
        verifyType(type);
        this.value = value;
    }

    private void verifyType(String type)
    {
        if (REGEX_TYPE.equalsIgnoreCase(type) || EQUALS_TYPE.equalsIgnoreCase(type))
        {
            return;
        }
        throw new IllegalArgumentException(type + " passed for metric filtering is not recognized. Expected types are "
                                           + REGEX_TYPE + " or " + EQUALS_TYPE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String type()
    {
        return type;
    }

    @JsonProperty(value = "type")
    public void setType(String type)
    {
        verifyType(type);
        this.type = type;
    }

    /**
     * {@inheritDoc}
     */
    public String value()
    {
        return value;
    }
}
