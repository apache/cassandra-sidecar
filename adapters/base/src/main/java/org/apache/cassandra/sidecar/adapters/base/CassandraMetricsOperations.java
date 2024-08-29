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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.response.ClientStatsResponse;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.MetricsOperations;

/**
 * An interface that pulls methods from the Cassandra Metrics Proxy
 */
public class CassandraMetricsOperations implements MetricsOperations
{
    static final String METRICS_SERVICE_OBJ_NAME = "org.apache.cassandra.metrics:type=%s,name=%s";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MMM dd, yyyy HH:mm:ss");
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraMetricsOperations.class);
    protected final JmxClient jmxClient;

    /**
     * Creates a new instance with the provided {@link JmxClient}
     *
     * @param jmxClient   the JMX client used to communicate with the Cassandra instance
     */
    public CassandraMetricsOperations(JmxClient jmxClient)
    {
        this.jmxClient = jmxClient;
    }

    /**
     * Retrieve the connected client stats metrics from the cluster
     * @param params the request parameters that are the filter options for the data returned
     * @return the requested client stats
     */
    public ClientStatsResponse clientStats(Map<String, String> params)
    {
        boolean isListConnections = Boolean.valueOf(params.getOrDefault("list-connections", "false"));
        boolean isVerbose = Boolean.valueOf(params.getOrDefault("verbose", "false"));
        boolean isByProtocol = Boolean.valueOf(params.getOrDefault("by-protocol", "false"));
        boolean isClientOptions = Boolean.valueOf(params.getOrDefault("client-options", "false"));

        ClientStatsResponse.Builder response = ClientStatsResponse.builder();

        JmxGaugeOperations gaugeMetric = jmxClient.proxy(JmxGaugeOperations.class, metricMBeanName("Client", "connections"));
        List<Map<String, String>> connectionStats = (List<Map<String, String>>) gaugeMetric.getValue();

        if (!connectionStats.isEmpty() && (isListConnections || isClientOptions || isVerbose))
        {
            populateConnectionStatsResponse(isVerbose, isClientOptions, connectionStats, response);
        }

        JmxGaugeOperations clientsByUser = jmxClient.proxy(JmxGaugeOperations.class, metricMBeanName("Client", "connectedNativeClientsByUser"));
        Map<String, Integer> nativeClientStats = (Map<String, Integer>) clientsByUser.getValue();
        if (!nativeClientStats.isEmpty())
        {
            int total = nativeClientStats.values().stream().reduce(0, (a, b) -> Integer.valueOf(a + b));
            response.totalConnectedClients(total);
            response.connectionsByUser(nativeClientStats);
        }

        if (isByProtocol)
        {
            JmxGaugeOperations clientsByProtocolVersion = jmxClient.proxy(JmxGaugeOperations.class, metricMBeanName("Client", "clientsByProtocolVersion"));
            List<Map<String, String>> clientsProtoVersionStats = (List<Map<String, String>>) clientsByProtocolVersion.getValue();
            if (!clientsProtoVersionStats.isEmpty())
            {
                populateProtocolVersionStatsResponse(clientsProtoVersionStats, response);
            }
        }
        return response.build();
    }

    private void populateProtocolVersionStatsResponse(List<Map<String, String>> clientsProtoVersionStats, ClientStatsResponse.Builder response)
    {
        for (Map<String, String> mapping : clientsProtoVersionStats)
        {
            for (Map.Entry<String, String> entry : mapping.entrySet())
            {
                switch (ClientStatsResponse.ProtocolVersion.valueOf(entry.getKey()))
                {
                    case protocolVersion: response.protocolVersion(entry.getValue()); break;
                    case inetAddress: response.remoteInetAddress(entry.getValue()); break;
                    case lastSeenTime:
                        Instant instant = Instant.ofEpochMilli(Long.parseLong(entry.getValue()));
                        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
                        String lastSeen = dateTime.format(formatter);
                        response.lastSeenTime(lastSeen);
                        break;
                    default:
                        LOGGER.error("Unrecognized protocol-version stat:{}", entry.getKey());

                }
            }
        }
    }

    private void populateConnectionStatsResponse(boolean isVerbose,
                                                 boolean isClientOptions,
                                                 List<Map<String, String>> connectionStats,
                                                 ClientStatsResponse.Builder response)
    {
        for (Map<String, String> conn : connectionStats)
        {
            for (Map.Entry<String, String> e: conn.entrySet())
            {
                switch(ClientStatsResponse.Connections.valueOf(e.getKey()))
                {
                    case address:
                        response.address(e.getValue());
                        break;
                    case user:
                        response.user(e.getValue());
                        break;
                    case version:
                        response.version(e.getValue());
                        break;
                    case clientOptions:
                        if (isClientOptions || isVerbose)
                        {
                            response.clientOptions(parseClientOptions(e.getValue()));
                        }
                        break;
                    case driverName:
                        response.driverName(e.getValue());
                        break;
                    case driverVersion:
                        response.driverVersion(e.getValue());
                        break;
                    case requests:
                        response.requests(e.getValue());
                        break;
                    case keyspace:
                        response.keyspace(e.getValue());
                        break;
                    case ssl:
                        response.ssl(Boolean.getBoolean(e.getValue()));
                        break;
                    case cipher:
                        response.cipher(e.getValue());
                        break;
                    case protocol:
                        response.protocol(e.getValue());
                        break;
                    case authenticationMode:
                        if (isVerbose)
                        {
                            response.authenticationMode(e.getValue());
                        }
                        break;
                    case authenticationMetadata:
                        if (isVerbose)
                        {
                            response.authenticationMetadata(e.getValue());
                        }
                        break;
                    default:
                        LOGGER.error("Unrecognized connection stat:{}", e.getKey());
                }
            }
        }
    }

    private static Map<String, String> parseClientOptions(String value)
    {
        if (value == null || value.trim().isEmpty())
        {
            // Handle the case where the input is null or empty
            return Collections.emptyMap();
        }
        return Arrays.stream(value.split(","))
                     .map(String::trim)
                     .map(s -> s.split("=", 2))
                     .filter(parts -> parts.length == 2 && !parts[0].trim().isEmpty() && !parts[1].trim().isEmpty())
                     .collect(Collectors.toMap(parts -> parts[0].trim(), parts -> parts[1].trim()));
    }


    private String metricMBeanName(String type, String metricName)
    {
        return String.format(METRICS_SERVICE_OBJ_NAME, type, metricName);
    }
}
