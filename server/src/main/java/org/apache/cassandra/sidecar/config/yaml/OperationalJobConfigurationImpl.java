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
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.OperationalJobConfiguration;

/**
 * Configuration for operational jobs managed by Sidecar
 */
public class OperationalJobConfigurationImpl implements OperationalJobConfiguration
{
    private static final SecondBoundConfiguration DEFAULT_TABLES_TTL = SecondBoundConfiguration.parse("90d");
    private static final SecondBoundConfiguration MIN_TABLES_TTL = SecondBoundConfiguration.parse("14d");

    protected SecondBoundConfiguration tablesTtl;

    public OperationalJobConfigurationImpl()
    {
        this(builder());
    }

    protected OperationalJobConfigurationImpl(Builder builder)
    {
        this.tablesTtl = builder.tablesTtl;
        validate();
    }

    private void validate()
    {
        if (tablesTtl.compareTo(MIN_TABLES_TTL) < 0)
        {
            throw new IllegalArgumentException("tablesTtl cannot be less than " + MIN_TABLES_TTL);
        }
    }

    @Override
    @JsonProperty(value = "tables_ttl")
    public SecondBoundConfiguration tablesTtl()
    {
        return tablesTtl;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code OperationalJobConfigurationImpl} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, OperationalJobConfigurationImpl>
    {
        private SecondBoundConfiguration tablesTtl = DEFAULT_TABLES_TTL;

        protected Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        public Builder tablesTtl(SecondBoundConfiguration tablesTtl)
        {
            return update(b -> b.tablesTtl = tablesTtl);
        }

        @Override
        public OperationalJobConfigurationImpl build()
        {
            return new OperationalJobConfigurationImpl(this);
        }
    }
}
