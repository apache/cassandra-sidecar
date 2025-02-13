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
package org.apache.cassandra.sidecar.db;

import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.ConfigsSchema;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.routes.cdc.ValidServices;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConfigAccessorImplTest
{
    @ParameterizedTest
    @EnumSource(ValidServices.class)
    public void getConfigs(ValidServices service)
    {
        Map<String, String> configs = Map.of("k1", "v1", "k2", "v2");
        ConfigAccessorImpl configAccessor = getConfigAccessor(service, configs, false);
        Map<String, String> configsFromCassandra = configAccessor.getConfig().getConfigs();
        Assertions.assertEquals(configs, configsFromCassandra);
    }

    @ParameterizedTest
    @EnumSource(ValidServices.class)
    public void testGetConfigsNoConfigsForServiceInTable(ValidServices service)
    {
        Map<String, String> configs = Map.of();
        ConfigAccessorImpl configAccessor = getConfigAccessor(service, configs, false);
        Map<String, String> configsFromCassandra = configAccessor.getConfig().getConfigs();
        Assertions.assertEquals(configs, configsFromCassandra);
    }

    @ParameterizedTest
    @EnumSource(ValidServices.class)
    public void testGetConfigsNoServiceInTable(ValidServices service)
    {
        Map<String, String> configs = Map.of();
        ConfigAccessorImpl configAccessor = getConfigAccessor(service, configs, true);
        Map<String, String> configsFromCassandra = configAccessor.getConfig().getConfigs();
        Assertions.assertEquals(configs, configsFromCassandra);
    }

    @ParameterizedTest
    @EnumSource(ValidServices.class)
    public void testInsertConfigs(ValidServices service)
    {
        Map<String, String> configs = Map.of("k1", "v1", "k2", "v2");
        ConfigAccessorImpl configAccessor = getConfigAccessor(service, configs, false);
        Map<String, String> configsFromCassandra = configAccessor.storeConfig(configs).getConfigs();
        Assertions.assertEquals(configs, configsFromCassandra);
    }

    @ParameterizedTest
    @EnumSource(ValidServices.class)
    public void testDeleteConfigDoesntFail(ValidServices service)
    {
        Map<String, String> configs = Map.of("k1", "v1", "k2", "v2");
        ConfigAccessorImpl configAccessor = getConfigAccessor(service, configs, false);
        configAccessor.deleteConfig();
    }

    private ConfigAccessorImpl getConfigAccessor(ValidServices service, Map<String, String> configs, boolean noRowsExist)
    {
        SidecarSchema mockSidecarSchema = mock(SidecarSchema.class);
        InstanceMetadataFetcher mockInstanceMetaDataFetcher = getMockInstanceMetaDataFetcher();
        ConfigsSchema mockConfigsSchema = getMockConfigsSchema();
        CQLSessionProvider mockCQLSessionProvider = getMockCQLSessionProvider(configs, noRowsExist);
        if (service.equals(ValidServices.CDC))
        {
            return new CdcConfigAccessor(mockInstanceMetaDataFetcher, mockConfigsSchema, mockCQLSessionProvider, mockSidecarSchema);
        }
        return new KafkaConfigAccessor(mockInstanceMetaDataFetcher, mockConfigsSchema, mockCQLSessionProvider, mockSidecarSchema);
    }

    private ConfigsSchema getMockConfigsSchema()
    {
        ConfigsSchema mockConfigsSchema = mock(ConfigsSchema.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(preparedStatement.bind()).thenReturn(mock(BoundStatement.class));
        when(preparedStatement.bind(any())).thenReturn(mock(BoundStatement.class));
        when(mockConfigsSchema.selectConfig()).thenReturn(preparedStatement);
        when(mockConfigsSchema.insertConfig()).thenReturn(preparedStatement);
        when(mockConfigsSchema.deleteConfig()).thenReturn(preparedStatement);
        return mockConfigsSchema;
    }

    private InstanceMetadataFetcher getMockInstanceMetaDataFetcher()
    {
        InstanceMetadata instanceMeta = mock(InstanceMetadata.class);

        InstanceMetadataFetcher instanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        when(instanceMetadataFetcher.instance(any())).thenReturn(instanceMeta);
        return instanceMetadataFetcher;
    }

    CQLSessionProvider getMockCQLSessionProvider(Map<String, String> configs, boolean noRowsExist)
    {
        ResultSet resultSet = mock(ResultSet.class);
        if (noRowsExist)
        {
            when(resultSet.one()).thenReturn(null);
        }
        else
        {
            Row row = mock(Row.class);
            when(row.getMap(anyString(), any(Class.class), any())).thenReturn(configs);
            when(resultSet.one()).thenAnswer(invocation -> row);
        }

        Session session = mock(Session.class);
        when(session.execute(any(Statement.class))).then(invocation -> resultSet);

        CQLSessionProvider cqlSession = mock(CQLSessionProvider.class);
        when(cqlSession.get()).thenReturn(session);
        when(cqlSession.getIfConnected()).thenReturn(session);
        return cqlSession;
    }
}
