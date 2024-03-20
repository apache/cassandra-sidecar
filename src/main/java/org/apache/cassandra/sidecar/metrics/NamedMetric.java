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
import java.util.function.Function;
import java.util.stream.Collectors;

import com.codahale.metrics.Metric;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;

/**
 * {@link NamedMetric} is for creating {@link Metric} with a structured name. Metric name should contain domain it
 * is captured for and optional tags for additional context.
 *
 * @param <T>   {@link NamedMetric} can be created for any {@link Metric} type. T represents the Metric type
 *              {@link NamedMetric} represents
 */
public class NamedMetric<T extends Metric>
{
    public final String canonicalName;
    public final T metric;

    private NamedMetric(Builder<T> builder)
    {
        this.canonicalName = builder.makeFullName();
        this.metric = builder.register(canonicalName);
    }

    public static <T extends Metric> Builder<T> builder(Function<String, T> metricCreator)
    {
        return new Builder<>(metricCreator);
    }

    /**
     * {@link NamedMetric} builder static inner class.
     *
     * @param <T>    {@link NamedMetric} can be created for any {@link Metric} type. T represents the Metric type
     *               {@link NamedMetric} represents
     */
    public static class Builder<T extends Metric> implements DataObjectBuilder<Builder<T>, NamedMetric<T>>
    {
        private final Function<String, T> metricCreator;
        private final List<Tag> tags = new ArrayList<>();
        private String domain;
        private String name;

        public Builder(Function<String, T> metricCreator)
        {
            this.metricCreator = metricCreator;
        }

        /**
         * Sets {@code domain} of metric and returns a reference to this builder to enable method chaining.
         *
         * @param domain domain metric is part of
         * @return a reference to this Builder
         */
        public Builder<T> withDomain(String domain)
        {
            return update(b -> b.domain = domain);
        }

        /**
         * Sets {@code name} of metric and returns a reference to this builder to enable method chaining.
         *
         * @param name metric name
         * @return a reference to this Builder
         */
        public Builder<T> withName(String name)
        {
            return update(b -> b.name = name);
        }

        /**
         * Additional name tag added to metric name for more clarity. Tags are usually added like,
         * component=data, route=/stream/component, etc. Returns a reference to this Builder enabling method chaining.
         *
         * @param tag tag added to {@code tags}
         * @return a reference to this Builder
         */
        public Builder<T> addTag(Tag tag)
        {
            return update(b -> b.tags.add(tag));
        }

        /**
         * Additional name tag added to metric name for more clarity. Tags are usually added like,
         * component=data, route=/stream/component, etc. Returns a reference to this Builder enabling method chaining.
         *
         * @param key key of tag to be added
         * @param value value of tag to be added
         * @return a reference to this Builder
         */
        public Builder<T> addTag(String key, String value)
        {
            return addTag(Tag.of(key, value));
        }

        @Override
        public Builder<T> self()
        {
            return this;
        }

        @Override
        public NamedMetric<T> build()
        {
            Objects.requireNonNull(metricCreator);
            Objects.requireNonNull(domain);
            Objects.requireNonNull(name);

            return new NamedMetric<>(this);
        }

        private String makeFullName()
        {
            String featurePart = domain + '.';
            String combinedTags = !tags.isEmpty() ? combineTags() + '.' : "";
            return featurePart + combinedTags + name;
        }

        private String combineTags()
        {
            return tags.stream().map(tag -> tag.key + '=' + tag.value).collect(Collectors.joining("."));
        }

        private T register(String metricName)
        {
            return metricCreator.apply(metricName);
        }
    }

    /**
     * Used for tagging {@link NamedMetric} for additional context.
     */
    public static class Tag
    {
        public final String key;
        public final String value;

        public static Tag of(String key, String value)
        {
            return new Tag(key, value);
        }

        private Tag(String key, String value)
        {
            this.key = Objects.requireNonNull(key, "Key can not be null");
            this.value = Objects.requireNonNull(value, "Value can not be null");
        }
    }
}
