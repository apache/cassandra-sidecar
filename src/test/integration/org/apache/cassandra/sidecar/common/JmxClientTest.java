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

import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.sidecar.common.utils.GossipInfoParser;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.CassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to ensure connectivity with the JMX client
 */
public class JmxClientTest
{
    private static final String SS_OBJ_NAME = "org.apache.cassandra.db:type=StorageService";

    @CassandraIntegrationTest
    void testJmxConnectivity(CassandraTestContext context) throws IOException
    {
        try (JmxClient jmxClient = createJmxClient(context))
        {
            String opMode = jmxClient.proxy(SSProxy.class, SS_OBJ_NAME)
                                     .getOperationMode();
            assertThat(opMode).isNotNull();
            assertThat(opMode).isIn("LEAVING", "JOINING", "NORMAL", "DECOMMISSIONED", "CLIENT");

            IUpgradeableInstance instance = context.cluster().getFirstRunningInstance();
            IInstanceConfig config = instance.config();
            assertThat(jmxClient.host()).isEqualTo(config.broadcastAddress().getAddress().getHostAddress());
            assertThat(jmxClient.port()).isEqualTo(config.jmxPort());
        }
    }

    @CassandraIntegrationTest
    void testGossipInfo(CassandraTestContext context) throws IOException
    {
        try (JmxClient jmxClient = createJmxClient(context))
        {
            FailureDetector proxy = jmxClient.proxy(FailureDetector.class,
                                                    "org.apache.cassandra.net:type=FailureDetector");
            String rawGossipInfo = proxy.getAllEndpointStates();
            assertThat(rawGossipInfo).isNotEmpty();
            Map<String, ?> gossipInfoMap = GossipInfoParser.parse(rawGossipInfo);
            assertThat(gossipInfoMap).isNotEmpty();
            gossipInfoMap.forEach((key, value) -> GossipInfoParser.isGossipInfoHostHeader(key));
        }
    }

    @CassandraIntegrationTest
    void testCorrectVersion(CassandraTestContext context) throws IOException
    {
        try (JmxClient jmxClient = createJmxClient(context))
        {
            jmxClient.proxy(SSProxy.class, SS_OBJ_NAME)
                     .refreshSizeEstimates();
        }
    }

    /**
     * An interface that pulls a method from the Cassandra Storage Service Proxy
     */
    public interface SSProxy
    {
        String getOperationMode();

        void refreshSizeEstimates();

        String getReleaseVersion();
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
