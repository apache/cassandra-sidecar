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

package org.apache.cassandra.sidecar.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import org.apache.cassandra.sidecar.config.MetricsFilteringConfiguration;

import static org.apache.cassandra.sidecar.config.yaml.MetricsFilteringConfigurationImpl.EQUALS_TYPE;
import static org.apache.cassandra.sidecar.config.yaml.MetricsFilteringConfigurationImpl.REGEX_TYPE;

/**
 * Filter for deciding whether a metric should be captured
 */
public abstract class MetricFilter
{
    /**
     * @return Boolean indicating whether the {@link MetricFilter} is satisfied for provided metric name.
     */
    public abstract boolean matches(String name);

    public static List<MetricFilter> parse(List<MetricsFilteringConfiguration> filterConfigurations)
    {
        if (filterConfigurations == null)
        {
            return Collections.emptyList();
        }
        List<MetricFilter> filters = new ArrayList<>();
        for (MetricsFilteringConfiguration filterConfiguration : filterConfigurations)
        {
            if (filterConfiguration.type().equalsIgnoreCase(REGEX_TYPE))
            {
                filters.add(new Regex(filterConfiguration.value()));
            }
            else if (filterConfiguration.type().equalsIgnoreCase(EQUALS_TYPE))
            {
                filters.add(new Equals(filterConfiguration.value()));
            }
        }
        return filters;
    }

    /**
     * Metric name based {@link MetricFilter} that checks if a metric name matches provided regex pattern
     */
    public static class Regex extends MetricFilter
    {
        private final Pattern pattern;

        public Regex(String regex)
        {
            Objects.requireNonNull(regex);
            this.pattern = Pattern.compile(regex);
        }

        @Override
        public boolean matches(String name)
        {
            return pattern.matcher(name).matches();
        }
    }

    /**
     * Metric name based {@link MetricFilter} that checks if a metric name exactly matches provided value
     */
    public static class Equals extends MetricFilter
    {
        private final String value;

        public Equals(String value)
        {
            Objects.requireNonNull(value);
            this.value = value;
        }

        @Override
        public boolean matches(String name)
        {
            return name.equals(value);
        }
    }
}
