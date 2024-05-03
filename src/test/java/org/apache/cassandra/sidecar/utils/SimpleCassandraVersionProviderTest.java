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

import org.apache.cassandra.sidecar.common.server.ICassandraFactory;
import org.apache.cassandra.sidecar.mocks.V30;
import org.apache.cassandra.sidecar.mocks.V40;
import org.apache.cassandra.sidecar.mocks.V41;

import static org.assertj.core.api.Assertions.assertThat;

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

}
