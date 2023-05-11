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

import java.util.Map;

import org.apache.cassandra.sidecar.common.testing.CassandraIntegrationTest;
import org.apache.cassandra.sidecar.common.testing.CassandraTestContext;
import org.apache.cassandra.sidecar.common.utils.GossipInfoParser;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to ensure connectivity with the JMX client
 */
public class JmxClientTest
{
    private static final String SS_OBJ_NAME = "org.apache.cassandra.db:type=StorageService";

    @CassandraIntegrationTest
    void testJmxConnectivity(CassandraTestContext context)
    {
        String opMode = context.jmxClient.proxy(SSProxy.class, SS_OBJ_NAME)
                                         .operationMode();
        assertThat(opMode).isNotNull();
        assertThat(opMode).isIn("LEAVING", "JOINING", "NORMAL", "DECOMMISSIONED", "CLIENT");
    }

    @CassandraIntegrationTest
    void testGossipInfo(CassandraTestContext context)
    {
        SSProxy proxy = context.jmxClient.proxy(SSProxy.class,
                                                "org.apache.cassandra.net:type=FailureDetector");
        String rawGossipInfo = proxy.allEndpointStatesWithPort();
        assertThat(rawGossipInfo).isNotEmpty();
        Map<String, ?> gossipInfoMap = GossipInfoParser.parse(rawGossipInfo);
        assertThat(gossipInfoMap).isNotEmpty();
        gossipInfoMap.forEach((key, value) -> GossipInfoParser.isGossipInfoHostHeader(key));
    }

    @CassandraIntegrationTest
    void testConsumerCall(CassandraTestContext context)
    {
        context.jmxClient.proxy(SSProxy.class, SS_OBJ_NAME)
                         .refreshSizeEstimates();
    }

    /**
     * An interface that pulls a method from the Cassandra Storage Service Proxy
     */
    public interface SSProxy
    {
        String operationMode();

        void refreshSizeEstimates();

        String allEndpointStatesWithPort();
    }
}
