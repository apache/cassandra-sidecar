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
package org.apache.cassandra.sidecar.cluster;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.metrics.instance.InstanceHealthMetrics;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;
import org.jetbrains.annotations.NotNull;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;

/**
 * The CassandraAdapterDelegateTest is responsible for testing the functionality of the CassandraAdapterDelegate with
 * isolated dependencies.
 */
public class CassandraAdapterDelegateTest
{
    private CassandraAdapterDelegate cassandraAdapterDelegate;

    @BeforeEach
    public void setUp()
    {
        Vertx mockVertx = getMockVertx();
        int cassandraInstanceId = 0;
        CassandraVersionProvider cassandraVersionProvider = Mockito.mock(CassandraVersionProvider.class);
        Metadata metadata = Mockito.mock(Metadata.class);
        CQLSessionProvider cqlSessionProvider = getMockCqlSessionProvider(metadata);
        JmxClient jmxClient = Mockito.mock(JmxClient.class);
        String host = "localhost";
        int port = 9042;
        DriverUtils driverUtils = getMockDriverUtils(host, port, metadata);
        String sidecarVersion = "0.2.0-SNAPSHOT";
        InstanceHealthMetrics instanceHealthMetrics = new InstanceHealthMetrics(new MetricRegistry());
        cassandraAdapterDelegate = new CassandraAdapterDelegate(mockVertx,
                                                                cassandraInstanceId,
                                                                cassandraVersionProvider,
                                                                cqlSessionProvider,
                                                                jmxClient,
                                                                driverUtils,
                                                                sidecarVersion,
                                                                host,
                                                                port,
                                                                instanceHealthMetrics
        );
    }

    private static @NotNull DriverUtils getMockDriverUtils(String host, int port, Metadata metadata)
    {
        DriverUtils driverUtils = Mockito.mock(DriverUtils.class);
        Host mockHost = Mockito.mock(Host.class);
        InetSocketAddress mockAddress = new InetSocketAddress(host, port);
        when(driverUtils.getHost(metadata, mockAddress)).thenReturn(mockHost);
        return driverUtils;
    }

    private static @NotNull CQLSessionProvider getMockCqlSessionProvider(Metadata metadata)
    {
        Session session = Mockito.mock(Session.class);
        Cluster cluster = Mockito.mock(Cluster.class);
        when(cluster.isClosed()).thenReturn(false);
        when(cluster.getMetadata()).thenReturn(metadata);
        when(session.getCluster()).thenReturn(cluster);
        PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
        when(preparedStatement.bind()).thenReturn(Mockito.mock(BoundStatement.class));
        when(session.prepare(any(String.class))).thenReturn(preparedStatement);

        Row row = Mockito.mock(Row.class);
        when(row.getString("name")).thenReturn("concurrent_reads");
        when(row.getString("value")).thenReturn("16");
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(resultSet.all()).thenReturn(List.of(row));
        when(resultSet.one()).thenReturn(row);
        when(session.execute(argThat((Statement s) ->
                (s instanceof SimpleStatement) && "SELECT name, value FROM system_views.settings".equals(
                        ((SimpleStatement) s).getQueryString()
                )
        )))
        .thenReturn(resultSet)
        .thenThrow(NoHostAvailableException.class);

        CQLSessionProvider cqlSessionProvider = Mockito.mock(CQLSessionProvider.class);
        when(cqlSessionProvider.get()).thenReturn(session);
        when(cqlSessionProvider.getIfConnected()).thenReturn(session);
        return cqlSessionProvider;
    }

    private static @NotNull Vertx getMockVertx()
    {
        Vertx mockVertx = Mockito.mock(Vertx.class);
        EventBus mockEventBus = Mockito.mock(EventBus.class);
        when(mockVertx.eventBus()).thenReturn(mockEventBus);
        return mockVertx;
    }

    @Test
    public void nativeProtocolHealthCheckWhenHealthCheckSucceedsShouldSetNodeSettingsFromCql()
    {
        cassandraAdapterDelegate.nativeProtocolHealthCheck();
        Map<String, String> actual = cassandraAdapterDelegate.v2NodeSettings();
        Map<String, String> expected = Map.of("concurrent_reads", "16");
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void v2NodeSettingsWhenHealthCheckHasNotRanShouldThrowCassandraUnavailableException()
    {
        Assertions.assertThrows(CassandraUnavailableException.class, () -> cassandraAdapterDelegate.v2NodeSettings());
    }

    @Test
    public void cqlNodeSettingsWhenHealthCheckSucceedsThenFailsShouldUnsetV2NodeSettings()
    {
        cassandraAdapterDelegate.nativeProtocolHealthCheck();
        Map<String, String> actual = cassandraAdapterDelegate.v2NodeSettings();
        Map<String, String> expected = Map.of("concurrent_reads", "16");
        Assertions.assertEquals(expected, actual);
        cassandraAdapterDelegate.v2NodeSettings();
        cassandraAdapterDelegate.nativeProtocolHealthCheck();
        Assertions.assertFalse(cassandraAdapterDelegate.isNativeUp());
        Assertions.assertThrows(CassandraUnavailableException.class, () -> cassandraAdapterDelegate.v2NodeSettings());
    }
}
