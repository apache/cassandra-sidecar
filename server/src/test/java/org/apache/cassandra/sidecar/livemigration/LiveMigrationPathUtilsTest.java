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
import java.util.Collections;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.mockito.Mockito;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_CDC_RAW_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_COMMITLOG_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_HINTS_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_SAVED_CACHES_DIR_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.Mockito.when;

class LiveMigrationPathUtilsTest
{
    private static final String fileName = "file1.db";

    @TempDir
    Path tempDir;

    @Test
    public void testGetLocalPath()
    {
        String cassandraHomeDir = tempDir.resolve("testGetLocalPath").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);

        validateLocalPath(instanceMetadata.dataDirs().get(0) + "/" + fileName,
                          LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/0/" + fileName,
                          instanceMetadata);
        validateLocalPath(instanceMetadata.cdcDir() + "/" + fileName,
                          LIVE_MIGRATION_CDC_RAW_DIR_PATH + "/0/" + fileName,
                          instanceMetadata);
        validateLocalPath(instanceMetadata.commitlogDir() + "/" + fileName,
                          LIVE_MIGRATION_COMMITLOG_DIR_PATH + "/0/" + fileName,
                          instanceMetadata);
        validateLocalPath(instanceMetadata.hintsDir() + "/" + fileName,
                          LIVE_MIGRATION_HINTS_DIR_PATH + "/0/" + fileName,
                          instanceMetadata);
        validateLocalPath(instanceMetadata.savedCachesDir() + "/" + fileName,
                          LIVE_MIGRATION_SAVED_CACHES_DIR_PATH + "/0/" + fileName,
                          instanceMetadata);
        validateLocalPath(instanceMetadata.localSystemDataFileDir() + "/" + fileName,
                          LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH + "/0/" + fileName,
                          instanceMetadata);
    }

    @Test
    public void testGetLocalPathInvalidDownloadUrls()
    {

        String cassandraHomeDir = tempDir.resolve("testGetLocalPath").toString();
        InstanceMetadata instanceMetadata = getInstanceMetadata(cassandraHomeDir);

        Function<String, String> localPath = (url) -> LiveMigrationPathUtils.getLocalPath(url, instanceMetadata);

        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath.apply(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/2/" + fileName));

        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath.apply(LIVE_MIGRATION_CDC_RAW_DIR_PATH + "/1/" + fileName));

        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath.apply(LIVE_MIGRATION_COMMITLOG_DIR_PATH + "/1/" + fileName));

        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath.apply(LIVE_MIGRATION_HINTS_DIR_PATH + "/1/" + fileName));

        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath.apply(LIVE_MIGRATION_SAVED_CACHES_DIR_PATH + "/1/" + fileName));
        assertThatIllegalArgumentException()
        .isThrownBy(() -> localPath.apply(LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH + "/1/" + fileName));
    }

    void validateLocalPath(String expectedPath, String fileDownloadUrl, InstanceMetadata instanceMetadata)
    {
        assertThat(LiveMigrationPathUtils.getLocalPath(fileDownloadUrl, instanceMetadata)).isEqualTo(expectedPath);
    }

    InstanceMetadata getInstanceMetadata(String cassandraHomeDir)
    {
        InstanceMetadata instanceMetadata = Mockito.mock(InstanceMetadata.class);
        when(instanceMetadata.dataDirs()).thenReturn(Collections.singletonList(cassandraHomeDir + "/data"));
        when(instanceMetadata.cdcDir()).thenReturn(cassandraHomeDir + "/cdc_raw");
        when(instanceMetadata.commitlogDir()).thenReturn(cassandraHomeDir + "/commitlog");
        when(instanceMetadata.hintsDir()).thenReturn(cassandraHomeDir + "/hints");
        when(instanceMetadata.savedCachesDir()).thenReturn(cassandraHomeDir + "/saved_caches");
        when(instanceMetadata.localSystemDataFileDir()).thenReturn(cassandraHomeDir + "/local_system_data");
        when(instanceMetadata.stagingDir()).thenReturn(cassandraHomeDir + "/sstable-staging");

        return instanceMetadata;
    }
}
