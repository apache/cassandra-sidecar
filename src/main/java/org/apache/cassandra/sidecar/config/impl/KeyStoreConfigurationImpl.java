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

package org.apache.cassandra.sidecar.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;

/**
 * Encapsulates key or trust store option configurations
 */
public class KeyStoreConfigurationImpl implements KeyStoreConfiguration
{
    public static final String DEFAULT_TYPE = "JKS";

    @JsonProperty("path")
    private final String path;

    @JsonProperty("password")
    private final String password;

    @JsonProperty(value = "type", defaultValue = DEFAULT_TYPE)
    private final String type;

    public KeyStoreConfigurationImpl()
    {
        this.path = null;
        this.password = null;
        this.type = DEFAULT_TYPE;
    }

    protected KeyStoreConfigurationImpl(Builder<?> builder)
    {
        path = builder.path;
        password = builder.password;
        type = builder.type;
    }

    /**
     * @return the path to the store
     */
    @Override
    @JsonProperty("path")
    public String path()
    {
        return path;
    }

    /**
     * @return the password for the store
     */
    @Override
    @JsonProperty("password")
    public String password()
    {
        return password;
    }

    /**
     * @return the type of the store
     */
    @Override
    @JsonProperty("type")
    public String type()
    {
        return type;
    }

    public static Builder<?> builder()
    {
        return new Builder<>();
    }

    /**
     * {@code KeyStoreConfigurationImpl} builder static inner class.
     * @param <T> the builder type
     */
    public static class Builder<T extends Builder<?>> implements DataObjectBuilder<T, KeyStoreConfigurationImpl>
    {
        protected String path;
        protected String password;
        protected String type = DEFAULT_TYPE;

        protected Builder()
        {
        }

        /**
         * Sets the {@code path} and returns a reference to this Builder enabling method chaining.
         *
         * @param path the {@code path} to set
         * @return a reference to this Builder
         */
        public T path(String path)
        {
            return update(b -> b.path = path);
        }

        /**
         * Sets the {@code password} and returns a reference to this Builder enabling method chaining.
         *
         * @param password the {@code password} to set
         * @return a reference to this Builder
         */
        public T password(String password)
        {
            return update(b -> b.password = password);
        }

        /**
         * Sets the {@code type} and returns a reference to this Builder enabling method chaining.
         *
         * @param type the {@code type} to set
         * @return a reference to this Builder
         */
        public T type(String type)
        {
            return update(b -> b.type = type);
        }

        /**
         * Returns a {@code KeyStoreConfigurationImpl} built from the parameters previously set.
         *
         * @return a {@code KeyStoreConfigurationImpl} built with parameters of this
         * {@code KeyStoreConfigurationImpl.Builder}
         */
        @Override
        public KeyStoreConfigurationImpl build()
        {
            return new KeyStoreConfigurationImpl(this);
        }
    }
}
