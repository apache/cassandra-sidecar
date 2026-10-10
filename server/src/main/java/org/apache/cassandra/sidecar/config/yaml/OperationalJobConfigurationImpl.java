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
    private static final boolean DEFAULT_COORDINATION_ENABLED = false;
    private static final SecondBoundConfiguration DEFAULT_LOCAL_JOB_COORDINATION_DELAY = SecondBoundConfiguration.parse("3m");
    private static final SecondBoundConfiguration DEFAULT_NODE_EXECUTION_TIMEOUT = SecondBoundConfiguration.parse("10m");
    private static final boolean DEFAULT_DURABLE_TRACKING_ENABLED = false;

    protected SecondBoundConfiguration tablesTtl;
    protected boolean coordinationEnabled;
    protected SecondBoundConfiguration localJobCoordinationDelay;
    protected SecondBoundConfiguration nodeExecutionTimeout;
    protected boolean durableTrackingEnabled;

    public OperationalJobConfigurationImpl()
    {
        this(builder());
    }

    protected OperationalJobConfigurationImpl(Builder builder)
    {
        this.tablesTtl = builder.tablesTtl;
        this.coordinationEnabled = builder.coordinationEnabled;
        this.localJobCoordinationDelay = builder.localJobCoordinationDelay;
        this.nodeExecutionTimeout = builder.nodeExecutionTimeout;
        this.durableTrackingEnabled = builder.durableTrackingEnabled;
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

    @JsonProperty(value = "tables_ttl")
    public void setTablesTtl(SecondBoundConfiguration tablesTtl)
    {
        this.tablesTtl = tablesTtl;
    }

    @Override
    @JsonProperty(value = "coordination_enabled")
    public boolean coordinationEnabled()
    {
        return coordinationEnabled;
    }

    @JsonProperty(value = "coordination_enabled")
    public void setCoordinationEnabled(boolean coordinationEnabled)
    {
        this.coordinationEnabled = coordinationEnabled;
    }

    @Override
    @JsonProperty(value = "local_job_coordination_delay")
    public SecondBoundConfiguration localJobCoordinationDelay()
    {
        return localJobCoordinationDelay;
    }

    @JsonProperty(value = "local_job_coordination_delay")
    public void setLocalJobCoordinationDelay(SecondBoundConfiguration localJobCoordinationDelay)
    {
        this.localJobCoordinationDelay = localJobCoordinationDelay;
    }

    @Override
    @JsonProperty(value = "node_execution_timeout")
    public SecondBoundConfiguration nodeExecutionTimeout()
    {
        return nodeExecutionTimeout;
    }

    @JsonProperty(value = "node_execution_timeout")
    public void setNodeExecutionTimeout(SecondBoundConfiguration nodeExecutionTimeout)
    {
        this.nodeExecutionTimeout = nodeExecutionTimeout;
    }

    @Override
    @JsonProperty(value = "durable_tracking_enabled")
    public boolean durableTrackingEnabled()
    {
        return durableTrackingEnabled;
    }

    @JsonProperty(value = "durable_tracking_enabled")
    public void setDurableTrackingEnabled(boolean durableTrackingEnabled)
    {
        this.durableTrackingEnabled = durableTrackingEnabled;
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
        private boolean coordinationEnabled = DEFAULT_COORDINATION_ENABLED;
        private SecondBoundConfiguration localJobCoordinationDelay = DEFAULT_LOCAL_JOB_COORDINATION_DELAY;
        private SecondBoundConfiguration nodeExecutionTimeout = DEFAULT_NODE_EXECUTION_TIMEOUT;
        private boolean durableTrackingEnabled = DEFAULT_DURABLE_TRACKING_ENABLED;

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

        public Builder coordinationEnabled(boolean coordinationEnabled)
        {
            return update(b -> b.coordinationEnabled = coordinationEnabled);
        }

        public Builder localJobCoordinationDelay(SecondBoundConfiguration localJobCoordinationDelay)
        {
            return update(b -> b.localJobCoordinationDelay = localJobCoordinationDelay);
        }

        public Builder nodeExecutionTimeout(SecondBoundConfiguration nodeExecutionTimeout)
        {
            return update(b -> b.nodeExecutionTimeout = nodeExecutionTimeout);
        }

        public Builder durableTrackingEnabled(boolean durableTrackingEnabled)
        {
            return update(b -> b.durableTrackingEnabled = durableTrackingEnabled);
        }

        @Override
        public OperationalJobConfigurationImpl build()
        {
            return new OperationalJobConfigurationImpl(this);
        }
    }
}
