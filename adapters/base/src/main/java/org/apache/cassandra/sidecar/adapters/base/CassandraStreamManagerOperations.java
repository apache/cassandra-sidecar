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

package org.apache.cassandra.sidecar.adapters.base;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.management.openmbean.CompositeData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.adapters.base.data.SessionInfo;
import org.apache.cassandra.sidecar.adapters.base.data.StreamState;
import org.apache.cassandra.sidecar.common.response.data.StreamProgressStats;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.StreamManagerOperations;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;

import static org.apache.cassandra.sidecar.adapters.base.StreamManagerJmxOperations.STREAM_MANAGER_OBJ_NAME;

/**
 * An implementation of the {@link StreamManagerOperations} that interfaces with Cassandra 4.0 and later
 */
public class CassandraStreamManagerOperations implements StreamManagerOperations
{

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStorageOperations.class);
    protected final JmxClient jmxClient;

    /**
     * Creates a new instance with the provided {@link JmxClient} and {@link DnsResolver}
     *
     * @param jmxClient   the JMX client used to communicate with the Cassandra instance
     * @param dnsResolver the DNS resolver used to lookup replicas
     */
    public CassandraStreamManagerOperations(JmxClient jmxClient, DnsResolver dnsResolver)
    {
        this.jmxClient = jmxClient;
    }

    public StreamProgressStats getStreamProgressStats()
    {
        Set<CompositeData> streamData = jmxClient.proxy(StreamManagerJmxOperations.class, STREAM_MANAGER_OBJ_NAME)
                                                 .getCurrentStreams();

        List<StreamState> streamStates = streamData.stream().map(StreamState::new).collect(Collectors.toList());
        return computeStats(streamStates);
    }

    private StreamProgressStats computeStats(List<StreamState> streamStates)
    {
        List<SessionInfo> sessions = streamStates.stream().map(s -> s.sessions()).flatMap(Collection::stream).collect(Collectors.toList());

        long totalFilesToReceive = 0;
        long totalFilesReceived = 0;
        long totalBytesToReceive = 0;
        long totalBytesReceived = 0;

        long totalFilesToSend = 0;
        long totalFilesSent = 0;
        long totalBytesToSend = 0;
        long totalBytesSent = 0;

        for (SessionInfo s : sessions)
        {
            totalBytesToReceive += s.getTotalSizeToReceive();
            totalBytesReceived += s.getTotalSizeReceived();
            totalFilesToReceive += s.getTotalFilesToReceive();
            totalFilesReceived += s.getTotalFilesReceived();
            totalBytesToSend += s.getTotalSizeToSend();
            totalBytesSent += s.getTotalSizeSent();
            totalFilesToSend += s.getTotalFilesToSend();
            totalFilesSent += s.getTotalFilesSent();

        }
        LOGGER.info("Progress Stats: {}, {}, {}, {}", totalBytesToReceive, totalBytesReceived, totalBytesToSend, totalBytesSent);
        return new StreamProgressStats(totalFilesToReceive, totalFilesReceived, totalBytesToReceive, totalBytesReceived,
                                       totalFilesToSend, totalFilesSent, totalBytesToSend, totalBytesSent);

    }
}
