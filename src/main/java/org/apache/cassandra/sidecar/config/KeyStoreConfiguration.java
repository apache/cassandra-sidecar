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

package org.apache.cassandra.sidecar.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;

/**
 * Encapsulates key or trust store option configurations
 */
public class KeyStoreConfiguration
{
    public static final String DEFAULT_TYPE = "JKS";

    @JsonProperty("path")
    private final String path;

    @JsonProperty("password")
    private final String password;

    @JsonProperty(value = "type", defaultValue = DEFAULT_TYPE)
    private final String type;

    public KeyStoreConfiguration()
    {
        this.path = null;
        this.password = null;
        this.type = DEFAULT_TYPE;
    }

    protected KeyStoreConfiguration(Builder builder)
    {
        path = builder.path;
        password = builder.password;
        type = builder.type;
    }

    /**
     * @return the path to the store
     */
    @JsonProperty("path")
    public String path()
    {
        return path;
    }

    /**
     * @return the password for the store
     */
    @JsonProperty("password")
    public String password()
    {
        return password;
    }

    /**
     * @return the type of the store
     */
    @JsonProperty("type")
    public String type()
    {
        return type;
    }

    /**
     * @return {@code true} if both {@link #path()} and {@link #password()} are provided
     */
    public boolean isConfigured()
    {
        return path != null && password != null;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code JksConfiguration} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, KeyStoreConfiguration>
    {
        private String path;
        private String password;
        private String type = DEFAULT_TYPE;

        protected Builder()
        {
        }

        /**
         * Sets the {@code path} and returns a reference to this Builder enabling method chaining.
         *
         * @param path the {@code path} to set
         * @return a reference to this Builder
         */
        public Builder path(String path)
        {
            return override(b -> b.path = path);
        }

        /**
         * Sets the {@code password} and returns a reference to this Builder enabling method chaining.
         *
         * @param password the {@code password} to set
         * @return a reference to this Builder
         */
        public Builder password(String password)
        {
            return override(b -> b.password = password);
        }

        /**
         * Sets the {@code type} and returns a reference to this Builder enabling method chaining.
         *
         * @param type the {@code type} to set
         * @return a reference to this Builder
         */
        public Builder type(String type)
        {
            return override(b -> b.type = type);
        }

        /**
         * Returns a {@code JksConfiguration} built from the parameters previously set.
         *
         * @return a {@code JksConfiguration} built with parameters of this {@code JksConfiguration.Builder}
         */
        @Override
        public KeyStoreConfiguration build()
        {
            return new KeyStoreConfiguration(this);
        }
    }
}
