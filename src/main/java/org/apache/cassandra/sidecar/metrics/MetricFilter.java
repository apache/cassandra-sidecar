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
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import org.apache.cassandra.sidecar.config.MetricsFilteringConfiguration;

/**
 * Filter for deciding whether a metric should be captured
 */
public abstract class MetricFilter
{
    public abstract boolean isAllowed(String name);

    public static List<MetricFilter> parse(List<MetricsFilteringConfiguration> filterConfigurations)
    {
        List<MetricFilter> filters = new ArrayList<>();
        for (MetricsFilteringConfiguration filterConfiguration : filterConfigurations)
        {
            if (filterConfiguration.type().equalsIgnoreCase("regex"))
            {
                filters.add(new Regex(filterConfiguration.pattern(), filterConfiguration.inverse()));
            }
            else if (filterConfiguration.type().equalsIgnoreCase("equals"))
            {
                filters.add(new Equals(filterConfiguration.pattern(), filterConfiguration.inverse()));
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
        private final boolean inverse;

        public Regex(String regex, boolean inverse)
        {
            Objects.requireNonNull(regex);
            this.pattern = Pattern.compile(regex);
            this.inverse = inverse;
        }

        public boolean isAllowed(String name)
        {
            if (inverse)
            {
                return !pattern.matcher(name).matches();
            }
            return pattern.matcher(name).matches();
        }
    }

    /**
     * Metric name based {@link MetricFilter} that checks if a metric name exactly matches provided value
     */
    public static class Equals extends MetricFilter
    {
        private final String value;
        private final boolean inverse;

        public Equals(String value, boolean inverse)
        {
            Objects.requireNonNull(value);
            this.value = value;
            this.inverse = inverse;
        }

        public boolean isAllowed(String name)
        {
            if (inverse)
            {
                return !name.equals(value);
            }
            return name.equals(value);
        }
    }
}
