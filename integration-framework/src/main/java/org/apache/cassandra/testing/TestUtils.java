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

package org.apache.cassandra.testing;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.sidecar.adapters.base.CassandraFactory;
import org.apache.cassandra.sidecar.adapters.cassandra41.Cassandra41Factory;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.db.schema.TableSchemaFetcher;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;

/**
 * Helper class for integration testing functionality
 */
public final class TestUtils
{
    public static final String TEST_KEYSPACE = "testkeyspace";
    public static final String TEST_TABLE_PREFIX = "testtable";
    private static final AtomicInteger TEST_TABLE_ID = new AtomicInteger(0);

    //    public static final int ROW_COUNT = 10_000;
    public static final int ROW_COUNT = 1_000;

    // Replication factor configurations used for tests
    public static final Map<String, Integer> DC1_RF1 = Collections.singletonMap("datacenter1", 1);
    public static final Map<String, Integer> DC1_RF3 = Collections.singletonMap("datacenter1", 3);
    public static final Map<String, Integer> DC1_RF2_DC2_RF2 = ImmutableMap.of("datacenter1", 2,
                                                                               "datacenter2", 2);
    public static final Map<String, Integer> DC1_RF3_DC2_RF3 = ImmutableMap.of("datacenter1", 3,
                                                                               "datacenter2", 3);

    /*
     * Creates the test table with read-repair disabled for the in-jvm-dtests to allow validation of data
     * on the nodes following bulk-writes without the chance of false-positives from replication resulting from
     * read repairs.
     */
    public static final String CREATE_TEST_TABLE_STATEMENT =
    "CREATE TABLE IF NOT EXISTS %s (id int, course text, marks int, PRIMARY KEY (id)) WITH read_repair='NONE';";

    private TestUtils()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static QualifiedName uniqueTestTableFullName(String keyspace)
    {
        return uniqueTestTableFullName(keyspace, TEST_TABLE_PREFIX);
    }

    public static QualifiedName uniqueTestTableFullName(String keyspace, String testTablePrefix)
    {
        return new QualifiedName(keyspace, testTablePrefix + TEST_TABLE_ID.getAndIncrement());
    }

    /**
     * @param keyspace    the name of the keyspace
     * @param tablePrefix the prefix for the table
     * @return a {@link QualifiedName} with quoted keyspace and quoted table
     */
    public static QualifiedName uniqueTestQuotedKeyspaceQuotedTableFullName(String keyspace, String tablePrefix)
    {
        return new QualifiedName(keyspace, tablePrefix + TEST_TABLE_ID.getAndIncrement(), true, true);
    }

    /**
     * @param keyspace    the name of the keyspace
     * @param tablePrefix the prefix for the table
     * @return a {@link QualifiedName} with unquoted keyspace and quoted table
     */
    public static QualifiedName uniqueTestKeyspaceQuotedTableFullName(String keyspace, String tablePrefix)
    {
        return new QualifiedName(keyspace, tablePrefix + TEST_TABLE_ID.getAndIncrement(), false, true);
    }

    /**
     * @param keyspace    the name of the keyspace
     * @return a {@link QualifiedName} with quoted keyspace and unquoted table
     */
    public static QualifiedName uniqueTestQuotedKeyspaceTableFullName(String keyspace)
    {
        return new QualifiedName(keyspace, TEST_TABLE_PREFIX + TEST_TABLE_ID.getAndIncrement(), true, false);
    }

    /**
     * @param dnsResolver        the DNS resolver instance to use
     * @param tableSchemaFetcher the table schema fetcher instance to use
     * @return the Cassandra Version provider configured with all existing factories
     */
    public static CassandraVersionProvider cassandraVersionProvider(DnsResolver dnsResolver, TableSchemaFetcher tableSchemaFetcher)
    {
        DriverUtils driverUtils = new DriverUtils();
        return new CassandraVersionProvider.Builder()
               .add(new CassandraFactory(dnsResolver, driverUtils, tableSchemaFetcher))
               .add(new Cassandra41Factory(dnsResolver, driverUtils, tableSchemaFetcher))
               .build();
    }

    /**
     * Defaults to run in-jvm dtest
     */
    public static void configureDefaultDTestJarProperties()
    {
        // Settings to reduce the test setup delay incurred if gossip is enabled
        System.setProperty("cassandra.ring_delay_ms", "5000"); // down from 30s default
        System.setProperty("cassandra.consistent.rangemovement", "false");
        System.setProperty("cassandra.consistent.simultaneousmoves.allow", "true");
        // End gossip delay settings
        // Set the location of dtest jars
        System.setProperty("cassandra.test.dtest_jar_path", System.getProperty("cassandra.test.dtest_jar_path", "dtest-jars"));
        // Disable tcnative in netty as it can cause jni issues and logs lots errors
        System.setProperty("cassandra.disable_tcactive_openssl", "true");
        // As we enable gossip by default, make the checks happen faster
        System.setProperty("cassandra.gossip_settle_min_wait_ms", "500"); // Default 5000
        System.setProperty("cassandra.gossip_settle_interval_ms", "250"); // Default 1000
        System.setProperty("cassandra.gossip_settle_poll_success_required", "6"); // Default 3
        // Disable direct memory allocator as it doesn't release properly
        System.setProperty("cassandra.netty_use_heap_allocator", "true");
        // NOTE: This setting is named opposite of what it does
        // Disable requiring native file hints, which allows some native functions to fail and the test to continue.
        System.setProperty("cassandra.require_native_file_hints", "true");
        // Disable all native stuff in Netty as streaming isn't functional with native enabled
        System.setProperty("shaded.io.netty.transport.noNative", "true");
        // Lifted from the Simulation runner (we're running into similar errors):
        // this property is used to allow non-members of the ring to exist in gossip without breaking RF changes
        // it would be nice not to rely on this, but hopefully we'll have consistent range movements before it matters
        System.setProperty("cassandra.allow_alter_rf_during_range_movement", "true");

        System.setProperty("cassandra.minimum_replication_factor", "1");
        // Uncomment the line below to debug SSL Handshake issues during tests
        //System.setProperty("javax.net.debug", "ssl:handshake");
    }
}
