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

package org.apache.cassandra.sidecar.handlers.livemigration;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.yaml.LiveMigrationConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationInvalidRequestException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class LiveMigrationMapSidecarConfigImplTest
{

    @TempDir
    private Path tempDirPath;

    @Test
    void testMigrationMap()
    {
        LiveMigrationConfiguration liveMigrationConfiguration =
        new LiveMigrationConfigurationImpl(Collections.emptySet(),
                                           Collections.emptySet(),
                                           Map.of("localhost1", "localhost4"),
                                           20);

        SidecarConfigurationImpl sidecarConfig =
        SidecarConfigurationImpl.builder()
                                .liveMigrationConfiguration(liveMigrationConfiguration).build();

        LiveMigrationMapSidecarConfigImpl migrationMapSidecarConfig =
        new LiveMigrationMapSidecarConfigImpl(sidecarConfig);

        InstanceMetadata localhost1Metadata = instanceMetadata("localhost1", 1);
        InstanceMetadata localhost2Metadata = instanceMetadata("localhost2", 2);
        InstanceMetadata localhost4Metadata = instanceMetadata("localhost4", 4);

        assertThat(migrationMapSidecarConfig.getMigrationMap()).isNotNull();
        assertThat(migrationMapSidecarConfig.getMigrationMap().result().size()).isEqualTo(1);

        assertThat(migrationMapSidecarConfig.isSource(localhost1Metadata).result()).isTrue();
        assertThat(migrationMapSidecarConfig.isDestination(localhost1Metadata).result()).isFalse();
        assertThat(migrationMapSidecarConfig.isAny(localhost1Metadata).result()).isTrue();
        assertThat(migrationMapSidecarConfig.getSource(localhost1Metadata.host()).cause())
        .isInstanceOf(LiveMigrationInvalidRequestException.class);

        assertThat(migrationMapSidecarConfig.isSource(localhost2Metadata).result()).isFalse();
        assertThat(migrationMapSidecarConfig.isDestination(localhost2Metadata).result()).isFalse();
        assertThat(migrationMapSidecarConfig.isAny(localhost2Metadata).result()).isFalse();
        assertThat(migrationMapSidecarConfig.getSource(localhost2Metadata.host()).cause())
        .isInstanceOf(LiveMigrationInvalidRequestException.class);

        assertThat(migrationMapSidecarConfig.isSource(localhost4Metadata).result()).isFalse();
        assertThat(migrationMapSidecarConfig.isDestination(localhost4Metadata).result()).isTrue();
        assertThat(migrationMapSidecarConfig.isAny(localhost4Metadata).result()).isTrue();
        assertThat(migrationMapSidecarConfig.getSource(localhost4Metadata.host()).result()).isEqualTo(localhost1Metadata.host());
    }

    @Test
    void testLiveMigrationNotConfigured()
    {
        LiveMigrationConfiguration liveMigrationConfiguration =
        new LiveMigrationConfigurationImpl(Collections.emptySet(),
                                           Collections.emptySet(),
                                           Map.of(),
                                           20);

        SidecarConfigurationImpl sidecarConfig =
        SidecarConfigurationImpl.builder()
                                .liveMigrationConfiguration(liveMigrationConfiguration).build();

        InstanceMetadata localhostMetadata = instanceMetadata("localhost1", 1);

        LiveMigrationMapSidecarConfigImpl migrationMapSidecarConfig =
        new LiveMigrationMapSidecarConfigImpl(sidecarConfig);

        assertThat(migrationMapSidecarConfig.isSource(localhostMetadata).result())
        .isEqualTo(false);
        assertThat(migrationMapSidecarConfig.isDestination(localhostMetadata).result())
        .isEqualTo(false);
        assertThat(migrationMapSidecarConfig.isAny(localhostMetadata).result())
        .isEqualTo(false);
        assertThat(migrationMapSidecarConfig.getSource(localhostMetadata.host()).cause())
        .isInstanceOf(LiveMigrationInvalidRequestException.class);

        assertThat(migrationMapSidecarConfig.getSource(null).cause())
        .isInstanceOf(IllegalArgumentException.class);
    }

    InstanceMetadata instanceMetadata(String host, int id)
    {
        return InstanceMetadataImpl.builder().host(host).id(id).port(9042)
                                   .storagePort(7000)
                                   .storageDir(tempDirPath.toAbsolutePath() + "/" + host)
                                   .metricRegistry(new MetricRegistry()).build();
    }
}
