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

import java.time.Duration;

import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.KeyStoreConfiguration;
import org.apache.cassandra.sidecar.config.SidecarClientConfiguration;


/**
 * Configuration for Sidecar client
 */
public class SidecarClientConfigurationImpl implements SidecarClientConfiguration
{
    @Override
    public boolean useSsl()
    {
        return false;
    }

    @Override
    public KeyStoreConfiguration keystore()
    {
        return null;
    }

    @Override
    public KeyStoreConfiguration truststore()
    {
        return null;
    }

    @Override
    public MillisecondBoundConfiguration requestTimeout()
    {
        return null;
    }

    @Override
    public MillisecondBoundConfiguration requestIdleTimeout()
    {
        return null;
    }

    @Override
    public int connectionPoolMaxSize()
    {
        return 0;
    }

    @Override
    public MillisecondBoundConfiguration connectionPoolCleanerPeriod()
    {
        return null;
    }

    @Override
    public int connectionPoolEventLoopSize()
    {
        return 0;
    }

    @Override
    public int connectionPoolMaxWaitQueueSize()
    {
        return 0;
    }

    @Override
    public int maxRetries()
    {
        return 0;
    }

    @Override
    public long retryDelayMillis()
    {
        return 0;
    }

    @Override
    public long maxRetryDelayMillis()
    {
        return 0;
    }

    @Override
    public Duration minimumHealthRetryDelay()
    {
        return null;
    }

    @Override
    public Duration maximumHealthRetryDelay()
    {
        return null;
    }
}
