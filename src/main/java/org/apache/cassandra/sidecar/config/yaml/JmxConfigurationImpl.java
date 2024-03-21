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
import org.apache.cassandra.sidecar.config.JmxConfiguration;

/**
 * General JMX connectivity configuration that is not instance-specific.
 */
public class JmxConfigurationImpl implements JmxConfiguration
{

    @JsonProperty("max_retries")
    protected final int maxRetries;

    @JsonProperty("retry_delay_millis")
    protected final long retryDelayMillis;

    public JmxConfigurationImpl()
    {
        this(3, 200L);
    }

    public JmxConfigurationImpl(int maxRetries, long retryDelayMillis)
    {
        this.maxRetries = maxRetries;
        this.retryDelayMillis = retryDelayMillis;
    }

    /**
     * @return the maximum number of connection retry attempts to make before failing
     */
    @Override
    @JsonProperty("max_retries")
    public int maxRetries()
    {
        return maxRetries;
    }

    /**
     * @return the delay, in milliseconds, between retry attempts
     */
    @Override
    @JsonProperty("retry_delay_millis")
    public long retryDelayMillis()
    {
        return retryDelayMillis;
    }
}
