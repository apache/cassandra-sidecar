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

package org.apache.cassandra.sidecar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;

import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;
import org.apache.cassandra.sidecar.common.MockCassandraFactory;
import org.apache.cassandra.sidecar.routes.HealthService;

import static org.mockito.Mockito.mock;

/**
 * Provides the basic dependencies for unit tests.
 */
public class TestModule extends AbstractModule
{
    private static final Logger logger = LoggerFactory.getLogger(TestModule.class);

    @Singleton
    @Provides
    public CassandraAdapterDelegate delegate()
    {
        return mock(CassandraAdapterDelegate.class);
    }

    @Singleton
    @Provides
    public HealthService healthService(CassandraAdapterDelegate delegate)
    {
        return new HealthService(delegate);
    }


    @Provides
    @Singleton
    public Configuration configuration()
    {
        return abstractConfig();
    }

    protected Configuration abstractConfig()
    {
        return new Configuration.Builder()
                           .setCassandraHost("INVALID_FOR_TEST")
                           .setCassandraPort(0)
                           .setHost("127.0.0.1")
                           .setPort(6475)
                           .setHealthCheckFrequency(1000)
                           .setSslEnabled(false)
                           .build();
    }

    /**
     * The Mock factory is used for testing purposes, enabling us to test all failures and possible results
     * @return
     */
    @Provides
    @Singleton
    public CassandraVersionProvider cassandraVersionProvider()
    {
        CassandraVersionProvider.Builder builder = new CassandraVersionProvider.Builder();
        builder.add(new MockCassandraFactory());
        return builder.build();
    }

}
