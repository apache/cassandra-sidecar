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

package org.apache.cassandra.sidecar.livemigration;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.mockito.Mockito;

import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.CDC_RAW_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.COMMITLOG_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.DATA_FILE_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.HINTS_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.LOCAL_SYSTEM_DATA_FILE_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.SAVED_CACHES_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.hasAnyPlaceholder;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.replacePlaceholder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.when;

class LiveMigrationPlaceholderUtilTest
{

    private static final String DATA_DIR = "data";
    private static final String HINTS_DIR = "hints";
    private static final String COMMITLOG_DIR = "commitlog";
    private static final String SAVED_CACHES_DIR = "saved_caches";
    private static final String CDC_RAW_DIR = "cdc_raw";
    private static final String LOCAL_SYSTEM_DIR = "local_system_data";
    private static final String STAGING_DIR = "sstable-staging";

    @TempDir
    Path tempDir;

    @Test
    public void testReplacePlaceholder()
    {
        assertThat(replacePlaceholder("/no/placeholder/dir", Collections.singleton("DATA_DIR"), "data_dir"))
        .isEqualTo("/no/placeholder/dir");

        assertThat(replacePlaceholder("${DATA_DIR}/ks1", Collections.singleton("DATA_DIR"), "/data_dir"))
        .isEqualTo("/data_dir/ks1");

        // replacement of placeholder happens if and only if the placeholder given is found.
        assertThat(replacePlaceholder("${HINTS_DIR}/ks1", Collections.singleton("DATA_DIR"), "/data_dir"))
        .isNull();
    }

    @Test
    void testHasAnyPlaceHolder()
    {
        assertThat(hasAnyPlaceholder("glob:/var/log/cassandra/data")).isFalse();
        assertThat(hasAnyPlaceholder("/var/log/cassandra/data")).isFalse();
        assertThat(hasAnyPlaceholder("glob:/var/log/cassandra/data/**")).isFalse();
        assertThat(hasAnyPlaceholder("glob:${SOME_DIR}/cassandra/data/**")).isTrue();
        assertThat(hasAnyPlaceholder("regex:${SOME_DIR_1}/cassandra/data/**")).isTrue();
        assertThat(hasAnyPlaceholder("${SOME_DIR_2}/cassandra/data/**")).isTrue();
        assertThat(hasAnyPlaceholder("{SOME_DIR_2}/cassandra/data/**")).isFalse();
    }

    @Test
    void testHasKnownPlaceHolder()
    {
        assertThat(hasAnyPlaceholder("${DATA_FILE_DIR}/ks/t1", Collections.singleton("HINTS_DIR"))).isFalse();
        assertThat(hasAnyPlaceholder("glob:${DATA_FILE_DIR}/ks/t1", Collections.singleton("HINTS_DIR"))).isFalse();


        assertThat(hasAnyPlaceholder("${HINTS_DIR}/ks/t1", Collections.singleton("HINTS_DIR"))).isTrue();
        assertThat(hasAnyPlaceholder("glob:${HINTS_DIR}/ks/t1", Collections.singleton("HINTS_DIR"))).isTrue();

        assertThat(hasAnyPlaceholder("${DATA_FILE_DIR}/ks/t1", Collections.singleton("DATA_FILE_DIR"))).isTrue();
        assertThat(hasAnyPlaceholder("glob:${DATA_FILE_DIR}/ks/t1", Collections.singleton("DATA_FILE_DIR"))).isTrue();
        assertThat(hasAnyPlaceholder("regex:${DATA_FILE_DIR}/k*/t1", Collections.singleton("DATA_FILE_DIR"))).isTrue();

        assertThat(hasAnyPlaceholder("glob:${DATA_FILE_DIR}/ks/t1", Collections.singleton("DATA_FILE_DIR_0"))).isFalse();
    }

    @Test
    public void testReplacePlaceholderUsingInstanceMetadata()
    {
        String cassandraHomeDir = tempDir.resolve("testReplacePlaceholderUsingInstanceMetadata").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);
        List<String> dataDirs = new ArrayList<>(2);
        String dataDir2 = DATA_DIR + "2";
        dataDirs.add(cassandraHomeDir + "/" + DATA_DIR);
        dataDirs.add(cassandraHomeDir + "/" + dataDir2);
        when(instanceMetadata.dataDirs()).thenReturn(dataDirs);

        assertThat(replacePlaceholder("glob:${" + HINTS_DIR_PLACEHOLDER + "}", instanceMetadata))
        .hasSize(1)
        .contains("glob:" + instanceMetadata.hintsDir());

        assertThat(replacePlaceholder("regex:${" + COMMITLOG_DIR_PLACEHOLDER + "}/Commitlog-7-1.log", instanceMetadata))
        .hasSize(1)
        .contains("regex:" + instanceMetadata.commitlogDir() + "/Commitlog-7-1.log");

        assertThat(replacePlaceholder("glob:${" + SAVED_CACHES_DIR_PLACEHOLDER + "}/cache.bin", instanceMetadata))
        .hasSize(1)
        .contains("glob:" + instanceMetadata.savedCachesDir() + "/cache.bin");

        assertThat(replacePlaceholder("glob:${" + DATA_FILE_DIR_PLACEHOLDER + "}/*/*/snapshots", instanceMetadata))
        .hasSize(2)
        .contains("glob:" + instanceMetadata.dataDirs().get(0) + "/*/*/snapshots")
        .contains("glob:" + instanceMetadata.dataDirs().get(1) + "/*/*/snapshots");

        assertThat(replacePlaceholder("glob:${" + DATA_FILE_DIR_PLACEHOLDER + "_0}/ks1/*", instanceMetadata))
        .hasSize(1)
        .contains("glob:" + instanceMetadata.dataDirs().get(0) + "/ks1/*");

        assertThat(replacePlaceholder("glob:${" + DATA_FILE_DIR_PLACEHOLDER + "_1}/k*/*", instanceMetadata))
        .hasSize(1)
        .contains("glob:" + instanceMetadata.dataDirs().get(1) + "/k*/*");

        assertThat(replacePlaceholder("glob:${" + CDC_RAW_DIR_PLACEHOLDER + "}/**", instanceMetadata))
        .hasSize(1)
        .contains("glob:" + instanceMetadata.cdcDir() + "/**");

        assertThat(replacePlaceholder("glob:${" + LOCAL_SYSTEM_DATA_FILE_DIR_PLACEHOLDER + "}/", instanceMetadata))
        .hasSize(1)
        .contains("glob:" + instanceMetadata.localSystemDataFileDir() + "/");

        // Trying to replace text not having any placeholder
        assertThat(replacePlaceholder("glob:/home/usr/data/*", instanceMetadata))
        .hasSize(1)
        .contains("glob:/home/usr/data/*");

        //Unknown placeholder
        assertThatIllegalArgumentException()
        .isThrownBy(() -> replacePlaceholder("glob:${UNKNOWN_PLACE_HOLDER}/0", instanceMetadata));
    }

    InstanceMetadata getInstanceMetadata(String cassandraHomeDir)
    {
        InstanceMetadata instanceMetadata = Mockito.mock(InstanceMetadata.class);
        when(instanceMetadata.dataDirs()).thenReturn(Collections.singletonList(cassandraHomeDir + "/" + DATA_DIR));
        when(instanceMetadata.cdcDir()).thenReturn(cassandraHomeDir + "/" + CDC_RAW_DIR);
        when(instanceMetadata.commitlogDir()).thenReturn(cassandraHomeDir + "/" + COMMITLOG_DIR);
        when(instanceMetadata.hintsDir()).thenReturn(cassandraHomeDir + "/" + HINTS_DIR);
        when(instanceMetadata.savedCachesDir()).thenReturn(cassandraHomeDir + "/" + SAVED_CACHES_DIR);
        when(instanceMetadata.localSystemDataFileDir()).thenReturn(cassandraHomeDir + "/" + LOCAL_SYSTEM_DIR);
        when(instanceMetadata.stagingDir()).thenReturn(cassandraHomeDir + "/" + STAGING_DIR);

        return instanceMetadata;
    }
}
