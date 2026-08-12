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
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.InstancesMetadataImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolvers;
import org.apache.cassandra.sidecar.config.CdcConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.yaml.CdcConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.db.CdcSystemViewsDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.SchemaUnavailableException;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.server.CdcMetrics;
import org.apache.cassandra.sidecar.metrics.server.ServerMetrics;
import org.apache.cassandra.sidecar.utils.CdcUtil;
import org.apache.cassandra.sidecar.utils.TimeProvider;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link CdcRawDirectorySpaceCleaner}
 */
class CdcRawDirectorySpaceCleanerTest
{
    private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
    private static final String TEST_SEGMENT_FILE_NAME_1 = "CommitLog-2-1250512736956320000.log";
    private static final String TEST_SEGMENT_FILE_NAME_2 = "CommitLog-2-1260512736956320000.log";
    private static final String TEST_SEGMENT_FILE_NAME_3 = "CommitLog-2-1340512736956320000.log";
    private static final String TEST_ORPHANED_SEGMENT_FILE_NAME = "CommitLog-2-1240512736956320000.log";
    private static final String TEST_INTACT_SEGMENT_FILE_NAME = "CommitLog-2-1340512736959990000.log";
    SidecarMetrics mockSidecarMetrics;
    CdcMetrics cdcMetrics;

    @BeforeEach
    void setup()
    {
        mockSidecarMetrics = mock(SidecarMetrics.class);
        ServerMetrics mockServerMetrics = mock(ServerMetrics.class);
        cdcMetrics = new CdcMetrics(METRIC_REGISTRY);
        when(mockSidecarMetrics.server()).thenReturn(mockServerMetrics);
        when(mockServerMetrics.cdc()).thenReturn(cdcMetrics);
    }

    @Test
    void testCdcRawDirectorySpaceCleaner(@TempDir Path tempDir) throws IOException
    {
        TimeProvider timeProvider = TimeProvider.DEFAULT_TIME_PROVIDER;
        CdcSystemViewsDatabaseAccessor systemViewsDatabaseAccessor = mock(CdcSystemViewsDatabaseAccessor.class);
        when(systemViewsDatabaseAccessor.getSettings(any()))
        .thenAnswer((Answer<Map<String, String>>) invocation -> Map.of("cdc_total_space", "1MiB"));
        when(systemViewsDatabaseAccessor.cdcTotalSpaceBytesSetting()).thenCallRealMethod();
        CdcConfiguration cdcConfiguration = new CdcConfigurationImpl();
        ServiceConfiguration serviceConfiguration = mock(ServiceConfiguration.class);
        when(serviceConfiguration.cdcConfiguration()).thenReturn(cdcConfiguration);

        InstancesMetadata instancesMetadata = mockInstanceMetadata(tempDir);
        CdcRawDirectorySpaceCleaner cleaner = new CdcRawDirectorySpaceCleaner(
        timeProvider,
        systemViewsDatabaseAccessor,
        serviceConfiguration,
        instancesMetadata,
        mockSidecarMetrics
        );

        checkExists(tempDir, TEST_ORPHANED_SEGMENT_FILE_NAME, true, false);
        checkExists(tempDir, TEST_SEGMENT_FILE_NAME_1);
        checkExists(tempDir, TEST_SEGMENT_FILE_NAME_2);
        checkExists(tempDir, TEST_SEGMENT_FILE_NAME_3);
        checkExists(tempDir, TEST_INTACT_SEGMENT_FILE_NAME, false, true);

        assertThat(cdcMetrics.criticalCdcRawSpace.metric.getValue()).isZero();
        assertThat(cdcMetrics.orphanedIdx.metric.getValue()).isZero();
        assertThat(cdcMetrics.deletedSegment.metric.getValue()).isZero();

        cleaner.routineCleanUp();

        // earliest cdc segment should be deleted along with orphaned idx file
        checkNotExists(tempDir, TEST_ORPHANED_SEGMENT_FILE_NAME);
        checkNotExists(tempDir, TEST_SEGMENT_FILE_NAME_1);

        // latest cdc segments should still exist as long as we have free buffer space
        checkExists(tempDir, TEST_SEGMENT_FILE_NAME_2);
        checkExists(tempDir, TEST_SEGMENT_FILE_NAME_3);
        checkExists(tempDir, TEST_INTACT_SEGMENT_FILE_NAME, false, true);

        // verify metrics match expected
        assertThat(cdcMetrics.criticalCdcRawSpace.metric.getValue()).isOne();
        assertThat(cdcMetrics.orphanedIdx.metric.getValue()).isOne();
        assertThat(cdcMetrics.totalCdcSpaceUsed.metric.getValue()).isGreaterThan(2097152L);
        // matches the "cdc_total_space": "1MiB" setting stubbed above
        assertThat(cdcMetrics.maxCdcSpaceConfigured.metric.getValue()).isEqualTo(1024L * 1024L);
        assertThat(cdcMetrics.deletedSegment.metric.getValue()).isGreaterThan(2097152L);
        assertThat(cdcMetrics.oldestSegmentAge.metric.getValue()).isZero();

        // delete all cdc files, in order to test the scenario that we do not have current cdc file, but have cdc file in the prior round.
        // We do not expect all CDC file to be cleaned up in a running system. But test it for robustness.
        Files.deleteIfExists(Paths.get(tempDir.toString(), CdcRawDirectorySpaceCleaner.CDC_DIR_NAME, TEST_INTACT_SEGMENT_FILE_NAME));
        cleaner.routineCleanUp(); // it should run fine.

        // configured limit is unaffected by usage dropping after the final cleanup
        assertThat(cdcMetrics.maxCdcSpaceConfigured.metric.getValue()).isEqualTo(1024L * 1024L);
    }

    @Test
    void testMaxUsageBytes()
    {
        FakeTimeProvider fakeTimeProvider = new FakeTimeProvider();
        InstancesMetadata instancesMetadata = mock(InstancesMetadata.class);
        CdcSystemViewsDatabaseAccessor mockCdcSystemViewsDatabaseAccessor = mock(CdcSystemViewsDatabaseAccessor.class);
        // First return 1MiB
        when(mockCdcSystemViewsDatabaseAccessor.cdcTotalSpaceBytesSetting()).thenReturn(1L << 20)
                                                                         // Next return 1GiB
                                                                         .thenReturn(1L << 30)
                                                                         // Next throw an exception when accessing Accessor layer
                                                                         .thenThrow(new SchemaUnavailableException("system_views", "settings"))
                                                                         // Next throw a runtime exception
                                                                         .thenThrow(new RuntimeException("error accessing data"))
                                                                         // Next return 1GiB
                                                                         .thenReturn(1L << 30);

        ServiceConfigurationImpl serviceConfiguration = new ServiceConfigurationImpl();
        CdcConfiguration cdcConfiguration = serviceConfiguration.cdcConfiguration();
        assertThat(cdcConfiguration).isNotNull();

        CdcRawDirectorySpaceCleaner cleaner = new CdcRawDirectorySpaceCleaner(
        fakeTimeProvider,
        mockCdcSystemViewsDatabaseAccessor,
        serviceConfiguration,
        instancesMetadata,
        mockSidecarMetrics
        );

        // start with no interactions
        verifyNoInteractions(mockCdcSystemViewsDatabaseAccessor);

        assertThat(cleaner.maxUsageBytes()).isEqualTo(1_024L * 1_024L);
        verify(mockCdcSystemViewsDatabaseAccessor, times(1)).cdcTotalSpaceBytesSetting();

        assertThat(cleaner.maxUsageBytes()).as("Should read from the cached value").isEqualTo(1_024L * 1_024L);
        verify(mockCdcSystemViewsDatabaseAccessor, times(1)).cdcTotalSpaceBytesSetting();

        // Advance the time provider to 1 millisecond before the cache expires
        fakeTimeProvider.advance(cdcConfiguration.cacheMaxUsage().toMillis() - 1, TimeUnit.MILLISECONDS);

        // Let's assert it again to ensure we are not reading from Accessor layer
        assertThat(cleaner.maxUsageBytes()).as("Should read from the cached value").isEqualTo(1_024L * 1_024L);
        verify(mockCdcSystemViewsDatabaseAccessor, times(1)).cdcTotalSpaceBytesSetting();

        // Now advance the time provider by the configured cache max usage
        fakeTimeProvider.advance(cdcConfiguration.cacheMaxUsage().toMillis(), TimeUnit.MILLISECONDS);

        // and we should now read 1GiB
        assertThat(cleaner.maxUsageBytes()).isEqualTo(1_024L * 1_024L * 1_024L);
        verify(mockCdcSystemViewsDatabaseAccessor, times(2)).cdcTotalSpaceBytesSetting();

        // Now advance the time provider by the configured cache max usage
        fakeTimeProvider.advance(cdcConfiguration.cacheMaxUsage().toMillis(), TimeUnit.MILLISECONDS);

        // we should fall back when a SchemaUnavailableException is thrown
        assertThat(cleaner.maxUsageBytes()).isEqualTo(cdcConfiguration.fallbackCdcRawDirectoryMaxSizeBytes());
        verify(mockCdcSystemViewsDatabaseAccessor, times(3)).cdcTotalSpaceBytesSetting();

        // also fall back when another exception is thrown
        assertThat(cleaner.maxUsageBytes()).isEqualTo(cdcConfiguration.fallbackCdcRawDirectoryMaxSizeBytes());
        verify(mockCdcSystemViewsDatabaseAccessor, times(4)).cdcTotalSpaceBytesSetting();

        // Finally, we recover and are able to read from accessor. Should read 1GiB
        assertThat(cleaner.maxUsageBytes()).isEqualTo(1_024L * 1_024L * 1_024L);
        verify(mockCdcSystemViewsDatabaseAccessor, times(5)).cdcTotalSpaceBytesSetting();

        // And read cached value
        assertThat(cleaner.maxUsageBytes()).isEqualTo(1_024L * 1_024L * 1_024L);
        verify(mockCdcSystemViewsDatabaseAccessor, times(5)).cdcTotalSpaceBytesSetting();
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

        when(instanceMetadata.dataDirs()).thenReturn(List.of(cdcDir.getParent() + "/data"));
        when(instanceMetadata.cdcDir()).thenReturn(cdcDir.getParent() + "/cdc_raw");
        return new InstancesMetadataImpl(instanceMetadata, DnsResolvers.DEFAULT);
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
            assertThat(f1.createNewFile()).isTrue();
            Files.write(f1.toPath(), RandomUtils.nextBytes(size));
        }

        if (!intact)
        {
            final File f2 = new File(cdcDir, CdcUtil.getIdxFileName(filename));
            assertThat(f2.createNewFile()).isTrue();
            Files.write(f2.toPath(), (size + (complete ? "\nCOMPLETED" : "")).getBytes(StandardCharsets.UTF_8));
        }
    }

    private void checkExists(Path tempDir, String logFileName)
    {
        checkExists(tempDir, logFileName, false, false);
    }

    private void checkExists(Path tempDir, String logFileName, boolean orphaned, boolean intact)
    {
        assertThat(Files.exists(Paths.get(tempDir.toString(), CdcRawDirectorySpaceCleaner.CDC_DIR_NAME, logFileName))).isEqualTo(!orphaned);
        assertThat(Files.exists(Paths.get(tempDir.toString(), CdcRawDirectorySpaceCleaner.CDC_DIR_NAME, CdcUtil.getIdxFileName(logFileName)))).isEqualTo(!intact);
    }

    private void checkNotExists(Path tempDir, String logFileName)
    {
        assertThat(Paths.get(tempDir.toString(), CdcRawDirectorySpaceCleaner.CDC_DIR_NAME, logFileName)).doesNotExist();
        assertThat(Paths.get(tempDir.toString(), CdcRawDirectorySpaceCleaner.CDC_DIR_NAME, CdcUtil.getIdxFileName(logFileName))).doesNotExist();
    }

    static class FakeTimeProvider implements TimeProvider
    {
        private final AtomicLong nanos = new AtomicLong(0);

        @Override
        public long currentTimeMillis()
        {
            return TimeUnit.NANOSECONDS.toMillis(nanos.get());
        }

        @Override
        public long nanoTime()
        {
            return nanos.get();
        }

        /**
         * Artificially advance time for a given {@code value} in the given {@link TimeUnit}
         *
         * @param value the value to advance
         * @param unit  the {@link TimeUnit} for the given {@code value}
         */
        public void advance(long value, TimeUnit unit)
        {
            nanos.addAndGet(unit.toNanos(value));
        }
    }
}
