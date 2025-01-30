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

package org.apache.cassandra.sidecar.config.yaml;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.config.InstanceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link InstanceConfigurationImpl} yaml parsing
 */
class InstanceConfigurationImplTest
{
    @Test
    void testParsing() throws IOException
    {
        String yaml = "cassandra_instances:\n" +
                      "  - id: 1\n" +
                      "    host: localhost1\n" +
                      "    port: 9042\n" +
                      "    storage_dir: /this/path/is/the/home/directory\n" +
                      "    staging_dir: ~/.ccm/test/node1/sstable-staging\n" +
                      "    cdc_dir: ~/.ccm/test/node1/cdc_raw";

        SidecarConfiguration config = SidecarConfigurationImpl.fromYamlString(yaml);
        assertThat(config.cassandraInstances()).hasSize(1);
        InstanceConfiguration instanceConfiguration = config.cassandraInstances().get(0);
        assertThat(instanceConfiguration.id()).isEqualTo(1);
        assertThat(instanceConfiguration.host()).isEqualTo("localhost1");
        assertThat(instanceConfiguration.port()).isEqualTo(9042);
        assertThat(instanceConfiguration.storageDir()).isEqualTo("/this/path/is/the/home/directory");
        assertThat(instanceConfiguration.stagingDir()).isEqualTo("~/.ccm/test/node1/sstable-staging");
        assertThat(instanceConfiguration.cdcDir()).isEqualTo("~/.ccm/test/node1/cdc_raw");
        assertThat(instanceConfiguration.dataDirs()).isNull();
        assertThat(instanceConfiguration.commitlogDir()).isNull();
        assertThat(instanceConfiguration.hintsDir()).isNull();
        assertThat(instanceConfiguration.savedCachesDir()).isNull();
        assertThat(instanceConfiguration.localSystemDataFileDir()).isNull();
    }
}
