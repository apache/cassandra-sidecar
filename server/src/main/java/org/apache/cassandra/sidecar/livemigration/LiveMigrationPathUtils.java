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

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_CDC_RAW_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_COMMITLOG_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_HINTS_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_SAVED_CACHES_DIR_PATH;

/**
 * Utility class for converting Live migration file download URLs to and from local file system paths.
 */
public class LiveMigrationPathUtils
{
    /**
     * Converts given live migration file download URL to local path.
     *
     * @param fileUrl  Live migration file download URL
     * @param metadata Cassandra instance metadata
     * @return local path for given live migration file download URL
     */
    public static String getLocalPath(@NotNull String fileUrl,
                                      @NotNull InstanceMetadata metadata)
    {

        Map<String, String> urlToLocalDirMap = migrationUrlLocalDirMap(metadata);
        for (Map.Entry<String, String> entry : urlToLocalDirMap.entrySet())
        {
            if (fileUrl.startsWith(entry.getKey()))
            {
                String relativePath = fileUrl.substring(entry.getKey().length());
                return Paths.get(entry.getValue(), relativePath).toAbsolutePath().toString();
            }
        }

        throw new IllegalArgumentException("File url " + fileUrl + " is unknown.");
    }

    private static Map<String, String> migrationUrlLocalDirMap(InstanceMetadata instanceMetadata)
    {
        Map<String, String> urlToLocalDirMap = new HashMap<String, String>()
        {{
            put(LIVE_MIGRATION_COMMITLOG_DIR_PATH + "/0/", instanceMetadata.commitlogDir());
            put(LIVE_MIGRATION_HINTS_DIR_PATH + "/0/", instanceMetadata.hintsDir());
            put(LIVE_MIGRATION_CDC_RAW_DIR_PATH + "/0/", instanceMetadata.cdcDir());
            put(LIVE_MIGRATION_SAVED_CACHES_DIR_PATH + "/0/", instanceMetadata.savedCachesDir());
            put(LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH + "/0/", instanceMetadata.localSystemDataFileDir());
        }};

        for (int i = 0; i < instanceMetadata.dataDirs().size(); i++)
        {
            urlToLocalDirMap.put(LIVE_MIGRATION_DATA_FILE_DIR_PATH + "/" + i + "/", instanceMetadata.dataDirs().get(0));
        }

        return urlToLocalDirMap;
    }
}
