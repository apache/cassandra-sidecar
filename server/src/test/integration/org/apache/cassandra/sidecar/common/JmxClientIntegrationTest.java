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

package org.apache.cassandra.sidecar.common;

import java.io.IOException;
import java.util.Map;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.utils.GossipInfoParser;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Test to ensure connectivity with the JMX client
 */
public class JmxClientIntegrationTest
{
    private static final String SS_OBJ_NAME = "org.apache.cassandra.db:type=StorageService";

    /**
     * Test jmx connectivity with various operations
     */
    @CassandraIntegrationTest
    void testJmxConnectivity(CassandraTestContext context) throws IOException
    {
        try (JmxClient jmxClient = createJmxClient(context))
        {
            testGetOperationMode(jmxClient, context.cluster());

            testGossipInfo(jmxClient);

            testCorrectVersion(jmxClient, String.valueOf(context.version.major));

            testTableCleanup(jmxClient, context.cluster());
        }
    }

    private void testGetOperationMode(JmxClient jmxClient, UpgradeableCluster cluster)
    {
        String opMode = jmxClient.proxy(SSProxy.class, SS_OBJ_NAME)
                                 .getOperationMode();
        assertThat(opMode).isNotNull();
        assertThat(opMode).isIn("LEAVING", "JOINING", "NORMAL", "DECOMMISSIONED", "CLIENT");

        IUpgradeableInstance instance = cluster.getFirstRunningInstance();
        IInstanceConfig config = instance.config();
        assertThat(jmxClient.host()).isEqualTo(config.broadcastAddress().getAddress().getHostAddress());
        assertThat(jmxClient.port()).isEqualTo(config.jmxPort());
    }

    private void testGossipInfo(JmxClient jmxClient)
    {
        FailureDetector proxy = jmxClient.proxy(FailureDetector.class,
                                                "org.apache.cassandra.net:type=FailureDetector");
        String rawGossipInfo = proxy.getAllEndpointStates();
        assertThat(rawGossipInfo).isNotEmpty();
        Map<String, ?> gossipInfoMap = GossipInfoParser.parse(rawGossipInfo);
        assertThat(gossipInfoMap).isNotEmpty();
        gossipInfoMap.forEach((key, value) -> GossipInfoParser.isGossipInfoHostHeader(key));
    }

    private void testCorrectVersion(JmxClient jmxClient, String majorVersion)
    {
        String releaseVersion = jmxClient.proxy(SSProxy.class, SS_OBJ_NAME)
                                         .getReleaseVersion();
        assertThat(releaseVersion).startsWith(majorVersion);
    }

    // a test to ensure the jmx client can invoke the MBean method
    private void testTableCleanup(JmxClient jmxClient, UpgradeableCluster cluster)
    {
        cluster.schemaChange("CREATE KEYSPACE jmx_client_test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}");
        cluster.schemaChange("CREATE TABLE jmx_client_test.table_cleanup ( a int PRIMARY KEY, b int)");
        cluster.get(1).executeInternal("INSERT INTO jmx_client_test.table_cleanup (a, b) VALUES (1, 1)");
        cluster.get(1).flush("jmx_client_test");
        int status = -1;
        try
        {
            status = jmxClient.proxy(SSProxy.class, SS_OBJ_NAME)
                              .forceKeyspaceCleanup(1, "jmx_client_test", "table_cleanup");
        }
        catch (Exception e)
        {
            fail("Unexpected exception ", e);
        }
        assertThat(status).isZero();

        assertThatThrownBy(() -> jmxClient.proxy(SSProxy.class, SS_OBJ_NAME)
                                          .forceKeyspaceCleanup(1, "jmx_client_test", "table_not_exist"))
        .hasMessageContaining("Unknown keyspace/cf pair");
    }

    /**
     * An interface that pulls a method from the Cassandra Storage Service Proxy
     */
    public interface SSProxy
    {
        String getOperationMode();

        void refreshSizeEstimates();

        String getReleaseVersion();

        int forceKeyspaceCleanup(int jobs, String keyspaceName, String... tables);
    }

    /**
     * An interface that pulls information from the Failure Detector MBean
     */
    public interface FailureDetector
    {
        String getAllEndpointStates();
    }

    private static JmxClient createJmxClient(CassandraTestContext context)
    {
        IUpgradeableInstance instance = context.cluster().getFirstRunningInstance();
        IInstanceConfig config = instance.config();
        return JmxClient.builder()
                        .host(config.broadcastAddress().getAddress().getHostAddress())
                        .port(config.jmxPort())
                        .build();
    }
}
