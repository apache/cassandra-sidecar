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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_FILES_ROUTE;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.CDC_RAW_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.COMMIT_LOG_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.DATA_FILE_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.HINTS_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.LOCAL_SYSTEM_DATA_FILE_DIR;
import static org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType.SAVED_CACHES_DIR;

/**
 * Utility class for creating test InstanceMetadata objects and live migration route paths for testing.
 */
public class InstanceMetadataTestUtil
{
    public static final String LIVE_MIGRATION_CDC_RAW_DIR_PATH = LIVE_MIGRATION_FILES_ROUTE + "/" + CDC_RAW_DIR.dirType;
    public static final String LIVE_MIGRATION_COMMITLOG_DIR_PATH = LIVE_MIGRATION_FILES_ROUTE + "/" + COMMIT_LOG_DIR.dirType;
    public static final String LIVE_MIGRATION_DATA_FILE_DIR_PATH = LIVE_MIGRATION_FILES_ROUTE + "/" + DATA_FILE_DIR.dirType;
    public static final String LIVE_MIGRATION_HINTS_DIR_PATH = LIVE_MIGRATION_FILES_ROUTE + "/" + HINTS_DIR.dirType;
    public static final String LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH = LIVE_MIGRATION_FILES_ROUTE
                                                                                + "/" + LOCAL_SYSTEM_DATA_FILE_DIR.dirType;
    public static final String LIVE_MIGRATION_SAVED_CACHES_DIR_PATH = LIVE_MIGRATION_FILES_ROUTE + "/" + SAVED_CACHES_DIR.dirType;
    private static final MetricRegistryFactory REGISTRY_FACTORY =
    new MetricRegistryFactory("test_metric_registry", Collections.emptyList(), Collections.emptyList());

    private InstanceMetadataTestUtil()
    {
        throw new AssertionError("Test utility class, no need to instantiate it");
    }

    public static InstanceMetadata getInstanceMetadata(String instanceIp,
                                                       int instanceId,
                                                       Path tempDir)
    {
        String root = tempDir.resolve(String.valueOf(instanceId)).toString();
        List<String> dataDirs = Arrays.asList(root + "/d1/data", root + "/d2/data");
        MetricRegistry instanceSpecificRegistry = REGISTRY_FACTORY.getOrCreate(instanceId);

        return InstanceMetadataImpl.builder()
                                   .id(instanceId)
                                   .host(instanceIp)
                                   .port(9042)
                                   .storagePort(7000)
                                   .dataDirs(dataDirs)
                                   .hintsDir(root + "/hints")
                                   .commitlogDir(root + "/commitlog")
                                   .savedCachesDir(root + "/saved_caches")
                                   .stagingDir(root + "/staging")
                                   .cdcDir(root + "/cdc")
                                   .localSystemDataFileDir(root + "/local_system_data")
                                   .metricRegistry(instanceSpecificRegistry)
                                   .build();
    }
}
