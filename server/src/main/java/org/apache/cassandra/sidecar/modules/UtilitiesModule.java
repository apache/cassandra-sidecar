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

package org.apache.cassandra.sidecar.modules;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.common.server.utils.SidecarVersionProvider;
import org.apache.cassandra.sidecar.utils.DigestAlgorithmProvider;
import org.apache.cassandra.sidecar.utils.JdkMd5DigestProvider;
import org.apache.cassandra.sidecar.utils.TimeProvider;
import org.apache.cassandra.sidecar.utils.XXHash32Provider;

/**
 * Provides the utilities
 */
public class UtilitiesModule extends AbstractModule
{
    @Provides
    @Singleton
    DriverUtils driverUtils()
    {
        return new DriverUtils();
    }

    @Provides
    @Singleton
    TimeProvider timeProvider()
    {
        return TimeProvider.DEFAULT_TIME_PROVIDER;
    }

    @Provides
    @Singleton
    DnsResolver dnsResolver()
    {
        return DnsResolver.DEFAULT;
    }

    /**
     * The provided hasher is used in {@link org.apache.cassandra.sidecar.restore.RestoreJobUtil}
     */
    @Provides
    @Singleton
    @Named("xxhash32")
    DigestAlgorithmProvider xxHash32Provider()
    {
        return new XXHash32Provider();
    }

    @Provides
    @Singleton
    @Named("md5")
    DigestAlgorithmProvider md5Provider()
    {
        return new JdkMd5DigestProvider();
    }

    @Provides
    @Singleton
    SidecarVersionProvider sidecarVersionProvider()
    {
        return new SidecarVersionProvider("/sidecar.version");
    }
}
