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

package org.apache.cassandra.sidecar.tasks;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.InstancesMetadataImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.config.CdcConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.CdcConfigurationImpl;
import org.apache.cassandra.sidecar.db.SystemViewsDatabaseAccessor;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.server.CdcMetrics;
import org.apache.cassandra.sidecar.metrics.server.ServerMetrics;
import org.apache.cassandra.sidecar.utils.CdcUtil;
import org.apache.cassandra.sidecar.utils.TimeProvider;
import org.mockito.stubbing.Answer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link CdcRawDirectorySpaceCleaner}
 */
public class CdcRawDirectorySpaceCleanerTest
{
    private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
    private static final String TEST_SEGMENT_FILE_NAME_1 = "CommitLog-2-1250512736956320000.log";
    private static final String TEST_SEGMENT_FILE_NAME_2 = "CommitLog-2-1260512736956320000.log";
    private static final String TEST_SEGMENT_FILE_NAME_3 = "CommitLog-2-1340512736956320000.log";
    private static final String TEST_ORPHANED_SEGMENT_FILE_NAME = "CommitLog-2-1240512736956320000.log";
    private static final String TEST_INTACT_SEGMENT_FILE_NAME = "CommitLog-2-1340512736959990000.log";

    @Test
    public void testCdcRawDirectorySpaceCleaner(@TempDir Path tempDir) throws IOException
    {
        TimeProvider timeProvider = TimeProvider.DEFAULT_TIME_PROVIDER;
        SystemViewsDatabaseAccessor systemViewsDatabaseAccessor = mock(SystemViewsDatabaseAccessor.class);
        when(systemViewsDatabaseAccessor.getSettings(any()))
        .thenAnswer((Answer<Map<String, String>>) invocation -> Map.of("cdc_total_space", "1MiB"));
        when(systemViewsDatabaseAccessor.getCdcTotalSpaceSetting()).thenCallRealMethod();
        CdcConfiguration cdcConfiguration = new CdcConfigurationImpl();
        ServiceConfiguration serviceConfiguration = mock(ServiceConfiguration.class);
        when(serviceConfiguration.cdcConfiguration()).thenReturn(cdcConfiguration);

        InstancesMetadata instancesMetadata = mockInstanceMetadata(tempDir);
        SidecarMetrics sidecarMetrics = mock(SidecarMetrics.class);
        ServerMetrics serverMetrics = mock(ServerMetrics.class);
        CdcMetrics cdcMetrics = new CdcMetrics(METRIC_REGISTRY);
        when(sidecarMetrics.server()).thenReturn(serverMetrics);
        when(serverMetrics.cdc()).thenReturn(cdcMetrics);
        CdcRawDirectorySpaceCleaner cleaner = new CdcRawDirectorySpaceCleaner(
        timeProvider,
        systemViewsDatabaseAccessor,
        serviceConfiguration,
        instancesMetadata,
        sidecarMetrics
        );

        checkExists(tempDir, TEST_ORPHANED_SEGMENT_FILE_NAME, true, false);
        checkExists(tempDir, TEST_SEGMENT_FILE_NAME_1);
        checkExists(tempDir, TEST_SEGMENT_FILE_NAME_2);
        checkExists(tempDir, TEST_SEGMENT_FILE_NAME_3);
        checkExists(tempDir, TEST_INTACT_SEGMENT_FILE_NAME, false, true);

        assertEquals(0L, cdcMetrics.criticalCdcRawSpace.metric.getValue());
        assertEquals(0L, cdcMetrics.orphanedIdx.metric.getValue());
        assertEquals(0L, cdcMetrics.deletedSegment.metric.getValue());

        cleaner.routineCleanUp();

        // earliest cdc segment should be deleted along with orphaned idx file
        checkNotExists(tempDir, TEST_ORPHANED_SEGMENT_FILE_NAME);
        checkNotExists(tempDir, TEST_SEGMENT_FILE_NAME_1);

        // latest cdc segments should still exist as long as we have free buffer space
        checkExists(tempDir, TEST_SEGMENT_FILE_NAME_2);
        checkExists(tempDir, TEST_SEGMENT_FILE_NAME_3);
        checkExists(tempDir, TEST_INTACT_SEGMENT_FILE_NAME, false, true);

        // verify metrics match expected
        assertEquals(1L, cdcMetrics.criticalCdcRawSpace.metric.getValue());
        assertEquals(1L, cdcMetrics.orphanedIdx.metric.getValue());
        assertTrue(cdcMetrics.totalCdcSpaceUsed.metric.getValue() > 2097152L);
        assertTrue(cdcMetrics.deletedSegment.metric.getValue() > 2097152L);
        assertEquals(0, cdcMetrics.oldestSegmentAge.metric.getValue());

        // delete all cdc files, in order to test the scenario that we do not have current cdc file, but have cdc file in the prior round.
        // We do not expect all CDC file to be cleaned up in a running system. But test it for robustness.
        Files.deleteIfExists(Paths.get(tempDir.toString(), CdcRawDirectorySpaceCleaner.CDC_DIR_NAME, TEST_INTACT_SEGMENT_FILE_NAME));
        cleaner.routineCleanUp(); // it should run fine.
    }

    /* test utils */

    private static InstancesMetadata mockInstanceMetadata(Path tempDir) throws IOException
    {
        InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);

        File cdcDir = Files.createDirectory(tempDir.resolve(CdcRawDirectorySpaceCleaner.CDC_DIR_NAME)).toFile();
        writeCdcSegment(cdcDir, TEST_ORPHANED_SEGMENT_FILE_NAME, 67108864, true, true, false);
        writeCdcSegment(cdcDir, TEST_SEGMENT_FILE_NAME_1, 2097152, true);
        writeCdcSegment(cdcDir, TEST_SEGMENT_FILE_NAME_2, 524288, true);
        writeCdcSegment(cdcDir, TEST_SEGMENT_FILE_NAME_3, 1024, false);

        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        writeCdcSegment(cdcDir, TEST_INTACT_SEGMENT_FILE_NAME, RandomUtils.nextInt(128, 256), false, false, true);

        when(instanceMetadata.dataDirs()).thenReturn(List.of(cdcDir.getParent()));
        return new InstancesMetadataImpl(instanceMetadata, DnsResolver.DEFAULT);
    }

    private static void writeCdcSegment(File cdcDir, String filename, int size, boolean complete) throws IOException
    {
        writeCdcSegment(cdcDir, filename, size, complete, false, false);
    }

    private static void writeCdcSegment(File cdcDir, String filename, int size, boolean complete, boolean orphaned, boolean intact) throws IOException
    {
        if (!orphaned)
        {
            final File f1 = new File(cdcDir, filename);
            assertTrue(f1.createNewFile());
            Files.write(f1.toPath(), RandomUtils.nextBytes(size));
        }

        if (!intact)
        {
            final File f2 = new File(cdcDir, CdcUtil.getIdxFileName(filename));
            assertTrue(f2.createNewFile());
            Files.write(f2.toPath(), (size + (complete ? "\nCOMPLETED" : "")).getBytes(StandardCharsets.UTF_8));
        }
    }

    private void checkExists(Path tempDir, String logFileName)
    {
        checkExists(tempDir, logFileName, false, false);
    }

    private void checkExists(Path tempDir, String logFileName, boolean orphaned, boolean intact)
    {
        assertEquals(!orphaned, Files.exists(Paths.get(tempDir.toString(), CdcRawDirectorySpaceCleaner.CDC_DIR_NAME, logFileName)));
        assertEquals(!intact, Files.exists(Paths.get(tempDir.toString(), CdcRawDirectorySpaceCleaner.CDC_DIR_NAME, CdcUtil.getIdxFileName(logFileName))));
    }

    private void checkNotExists(Path tempDir, String logFileName)
    {
        assertFalse(Files.exists(Paths.get(tempDir.toString(), CdcRawDirectorySpaceCleaner.CDC_DIR_NAME, logFileName)));
        assertFalse(Files.exists(Paths.get(tempDir.toString(), CdcRawDirectorySpaceCleaner.CDC_DIR_NAME, CdcUtil.getIdxFileName(logFileName))));
    }
}
