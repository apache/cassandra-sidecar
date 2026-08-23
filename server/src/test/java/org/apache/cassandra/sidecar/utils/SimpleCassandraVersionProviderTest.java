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

package org.apache.cassandra.sidecar.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.adapters.base.CassandraFactory;
import org.apache.cassandra.sidecar.adapters.cassandra41.Cassandra41Factory;
import org.apache.cassandra.sidecar.adapters.cassandra50.Cassandra50Factory;
import org.apache.cassandra.sidecar.adapters.cassandra60.Cassandra60Factory;
import org.apache.cassandra.sidecar.common.server.ICassandraFactory;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.db.schema.TableSchemaFetcher;
import org.apache.cassandra.sidecar.mocks.V30;
import org.apache.cassandra.sidecar.mocks.V40;
import org.apache.cassandra.sidecar.mocks.V41;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class SimpleCassandraVersionProviderTest
{

    CassandraVersionProvider.Builder builder;
    CassandraVersionProvider provider;

    @BeforeEach
    void setupBuilder()
    {
        builder = new CassandraVersionProvider.Builder();
        provider = builder.add(new V30())
                          .add(new V40())
                          .add(new V41()).build();
    }

    @Test
    void simpleTest()
    {
        ICassandraFactory cassandra = provider.cassandra(SimpleCassandraVersion.create("3.0.1"));
        assertThat(cassandra).hasSameClassAs(new V30());
    }

    @Test
    void equalityTest()
    {
        ICassandraFactory cassandra = provider.cassandra(SimpleCassandraVersion.create("3.0.0"));
        assertThat(cassandra).hasSameClassAs(new V30());
    }

    @Test
    void equalityTest2()
    {
        ICassandraFactory cassandra = provider.cassandra(SimpleCassandraVersion.create("4.0.0"));
        assertThat(cassandra).hasSameClassAs(new V40());
    }

    @Test
    void ensureHighVersionsWork()
    {
        ICassandraFactory cassandra = provider.cassandra(SimpleCassandraVersion.create("10.0.0"));
        assertThat(cassandra).hasSameClassAs(new V41());
    }

    @Test
    void ensureOutOfOrderInsertionWorks()
    {
        builder = new CassandraVersionProvider.Builder();
        provider = builder.add(new V40())
                          .add(new V41())
                          .add(new V30()).build();

        ICassandraFactory cassandra = provider.cassandra(SimpleCassandraVersion.create("4.0.0"));
        assertThat(cassandra).hasSameClassAs(new V40());
    }

    @Test
    void ensureDescendingInsertionWorks()
    {
        // the requested version is below the first factory registered, which the seeding in cassandra() would
        // otherwise return
        provider = new CassandraVersionProvider.Builder().add(new V41())
                                                         .add(new V40())
                                                         .add(new V30()).build();

        assertThat(provider.cassandra(SimpleCassandraVersion.create("4.0.7"))).hasSameClassAs(new V40());
        assertThat(provider.cassandra(SimpleCassandraVersion.create("3.0.7"))).hasSameClassAs(new V30());
    }

    @Test
    void ensureProductionFactoriesMatchTheirReleaseLine()
    {
        DnsResolver dnsResolver = mock(DnsResolver.class);
        DriverUtils driverUtils = new DriverUtils();
        TableSchemaFetcher tableSchemaFetcher = mock(TableSchemaFetcher.class);
        CassandraVersionProvider productionProvider
        = new CassandraVersionProvider.Builder()
          .add(new CassandraFactory(dnsResolver, driverUtils, tableSchemaFetcher))
          .add(new Cassandra41Factory(dnsResolver, driverUtils, tableSchemaFetcher))
          .add(new Cassandra50Factory(dnsResolver, driverUtils, tableSchemaFetcher))
          .add(new Cassandra60Factory(dnsResolver, driverUtils, tableSchemaFetcher))
          .build();

        assertThat(productionProvider.cassandra(SimpleCassandraVersion.create("4.0.13")))
        .isExactlyInstanceOf(CassandraFactory.class);
        assertThat(productionProvider.cassandra(SimpleCassandraVersion.create("4.1.11")))
        .isExactlyInstanceOf(Cassandra41Factory.class);
        assertThat(productionProvider.cassandra(SimpleCassandraVersion.create("5.0.6")))
        .isExactlyInstanceOf(Cassandra50Factory.class);
        assertThat(productionProvider.cassandra(SimpleCassandraVersion.create("6.0.1")))
        .isExactlyInstanceOf(Cassandra60Factory.class);
        // a pre-release node reports 6.0-alpha3-SNAPSHOT, which is the string that selects the adapter in practice
        assertThat(productionProvider.cassandra(SimpleCassandraVersion.create("6.0-alpha3-SNAPSHOT")))
        .isExactlyInstanceOf(Cassandra60Factory.class);
        assertThat(productionProvider.cassandra(SimpleCassandraVersion.create("6.0")))
        .isExactlyInstanceOf(Cassandra60Factory.class);
    }

}
