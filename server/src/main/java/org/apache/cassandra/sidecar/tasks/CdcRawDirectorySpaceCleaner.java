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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.config.CdcConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.db.CdcSystemViewsDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.SchemaUnavailableException;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.server.CdcMetrics;
import org.apache.cassandra.sidecar.utils.CdcUtil;
import org.apache.cassandra.sidecar.utils.FileUtils;
import org.apache.cassandra.sidecar.utils.TimeProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.sidecar.utils.CdcUtil.isLogFile;
import static org.apache.cassandra.sidecar.utils.CdcUtil.parseSegmentId;

/**
 * PeriodTask to monitor and remove the oldest commit log segments in the `cdc_raw` directory
 * when the space used hits the `cdc_total_space` limit set in the yaml file.
 */
public class CdcRawDirectorySpaceCleaner implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CdcRawDirectorySpaceCleaner.class);

    public static final String CDC_DIR_NAME = "cdc_raw";

    private final TimeProvider timeProvider;
    private final CdcSystemViewsDatabaseAccessor systemViewsDatabaseAccessor;
    private final CdcConfiguration cdcConfiguration;
    private final InstancesMetadata instancesMetadata;
    private final CdcMetrics cdcMetrics;

    // non-volatile variables, PeriodicTaskExecutor should ensure memory visibility
    @Nullable
    private Long maxUsageBytes = null;
    // lazily loaded from system_views.settings if available
    private Long maxUsageLastReadNanos = null;
    // cdc file -> file size in bytes. It memorizes the file set of the last time the checker runs.
    private Map<CdcRawSegmentFile, Long> priorCdcFiles = new HashMap<>();

    @Inject
    public CdcRawDirectorySpaceCleaner(TimeProvider timeProvider,
                                       CdcSystemViewsDatabaseAccessor systemViewsDatabaseAccessor,
                                       ServiceConfiguration serviceConfiguration,
                                       InstancesMetadata instancesMetadata,
                                       SidecarMetrics metrics)
    {
        this.timeProvider = timeProvider;
        this.systemViewsDatabaseAccessor = systemViewsDatabaseAccessor;
        this.cdcConfiguration = serviceConfiguration.cdcConfiguration();
        this.instancesMetadata = instancesMetadata;
        this.cdcMetrics = metrics.server().cdc();
    }

    @Override
    public DurationSpec delay()
    {
        return cdcConfiguration.cdcRawDirectorySpaceCleanerFrequency();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        try
        {
            routineCleanUp();
            promise.tryComplete();
        }
        catch (Throwable t)
        {
            LOGGER.warn("Failed to perform routine clean-up of cdc_raw directory", t);
            cdcMetrics.cdcRawCleanerFailed.metric.update(1L);
            promise.fail(t);
        }
    }

    /**
     * @return true if we need to refresh the cached `cdc_total_space` value.
     */
    protected boolean shouldRefreshCachedMaxUsage()
    {
        return maxUsageLastReadNanos == null ||
               (timeProvider.nanoTime() - maxUsageLastReadNanos) >=
               TimeUnit.MILLISECONDS.toNanos(cdcConfiguration.cacheMaxUsage().toMillis());
    }

    protected long maxUsageBytes()
    {
        if (!shouldRefreshCachedMaxUsage())
        {
            return Objects.requireNonNull(maxUsageBytes,
                                          "maxUsageBytes cannot be null if maxUsageLastReadNanos is non-null");
        }

        try
        {
            Long newValue = systemViewsDatabaseAccessor.cdcTotalSpaceBytesSetting();
            if (newValue != null)
            {
                if (!newValue.equals(maxUsageBytes))
                {
                    LOGGER.info("Change in cdc_total_space from system_views.settings prev={} latest={}",
                                maxUsageBytes, newValue);
                    this.maxUsageBytes = newValue;
                }
                this.maxUsageLastReadNanos = timeProvider.nanoTime();
                return this.maxUsageBytes;
            }
        }
        catch (SchemaUnavailableException e)
        {
            LOGGER.debug("Could not read cdc_total_space from system_views.settings", e);
        }
        catch (Throwable t)
        {
            LOGGER.error("Error reading cdc_total_space from system_views.settings", t);
        }

        LOGGER.warn("Could not read cdc_total_space from system_views.settings, falling back to props");
        return cdcConfiguration.fallbackCdcRawDirectoryMaxSizeBytes();
    }

    @Override
    public ScheduleDecision scheduleDecision()
    {
        if (cdcConfiguration.enableCdcRawDirectoryRoutineCleanUp())
        {
            return ScheduleDecision.EXECUTE;
        }
        LOGGER.debug("Skipping CdcRawDirectorySpaceCleaner: feature is disabled");
        return ScheduleDecision.SKIP;
    }

    protected void routineCleanUp()
    {
        for (InstanceMetadata instanceMetadata : instancesMetadata.instances())
        {
            String cdcDir = instanceMetadata.cdcDir();
            if (cdcDir != null)
            {
                try
                {
                    cleanUpCdcRawDirectory(new File(cdcDir));
                }
                catch (Exception e)
                {
                    LOGGER.warn("Unable to clean up CDC directory {} for instance {}", cdcDir, instanceMetadata, e);
                }
            }
            else
            {
                LOGGER.warn("CDC directory is not configured for instance {}. Skipping clean up", instanceMetadata);
            }
        }
    }

    protected void cleanUpCdcRawDirectory(File cdcRawDirectory)
    {
        if (!cdcRawDirectory.exists() || !cdcRawDirectory.isDirectory())
        {
            LOGGER.debug("Skipping CdcRawDirectorySpaceCleaner: CDC directory does not exist: {}", cdcRawDirectory);
            return;
        }

        List<CdcRawSegmentFile> segmentFiles = Optional
                                               .ofNullable(
                                               cdcRawDirectory.listFiles(this::validSegmentFilter))
                                               .map(files -> Arrays.stream(files)
                                                                   .map(CdcRawSegmentFile::new)
                                                                   .filter(
                                                                   CdcRawSegmentFile::indexExists)
                                                                   .collect(Collectors.toList())
                                               )
                                               .orElseGet(List::of);
        publishCdcStats(segmentFiles);
        if (segmentFiles.size() < 2)
        {
            LOGGER.debug("Skipping cdc data cleaner routine cleanup: No cdc data or only one single cdc segment is found.");
            return;
        }

        long directorySizeBytes = FileUtils.directorySizeBytes(cdcRawDirectory);
        long maxUsageBytes = maxUsageBytes();
        long upperLimitBytes = (long) (maxUsageBytes * cdcConfiguration.cdcRawDirectoryMaxPercentUsage());
        // Sort the files by segmentId to delete commit log segments in write order
        // The latest file is the current active segment, but it could be created before the retention duration, e.g. slow data ingress
        Collections.sort(segmentFiles);
        long nowInMillis = timeProvider.currentTimeMillis();

        // track the age of the oldest commit log segment to give indication of the time-window buffer available
        cdcMetrics.oldestSegmentAge.metric.setValue((int) MILLISECONDS.toSeconds(nowInMillis - segmentFiles.get(0).lastModified()));

        LOGGER.debug("Cdc data cleaner directorySizeBytes={} maxedUsageBytes={} upperLimitBytes={}",
                     directorySizeBytes, maxUsageBytes, upperLimitBytes);

        if (directorySizeBytes > upperLimitBytes)
        {
            if (segmentFiles.get(0).segmentId > segmentFiles.get(1).segmentId)
            {
                LOGGER.error("Cdc segments sorted incorrectly {} before {}",
                             segmentFiles.get(0).segmentId, segmentFiles.get(1).segmentId);
            }

            long criticalMillis = cdcConfiguration.cdcRawDirectoryCriticalBufferWindow().toMillis();
            long lowMillis = cdcConfiguration.cdcRawDirectoryLowBufferWindow().toMillis();

            // we keep the last commit log segment as it may still be actively written to
            int i = 0;
            while (i < segmentFiles.size() - 1 && directorySizeBytes > upperLimitBytes)
            {
                CdcRawSegmentFile segment = segmentFiles.get(i);
                long ageMillis = nowInMillis - segment.lastModified();

                if (ageMillis < criticalMillis)
                {
                    LOGGER.error("Insufficient Cdc buffer size to maintain {}-minute window segment={} maxSize={} ageMinutes={}",
                                 MILLISECONDS.toMinutes(criticalMillis), segment, upperLimitBytes,
                                 MILLISECONDS.toMinutes(ageMillis));
                    cdcMetrics.criticalCdcRawSpace.metric.update(1);
                }
                else if (ageMillis < lowMillis)
                {
                    LOGGER.warn("Insufficient Cdc buffer size to maintain {}-minute window segment={} maxSize={} ageMinutes={}",
                                MILLISECONDS.toMinutes(lowMillis), segment, upperLimitBytes,
                                MILLISECONDS.toMinutes(ageMillis));
                    cdcMetrics.lowCdcRawSpace.metric.update(1);
                }
                long length = 0;
                try
                {
                    length = deleteSegment(segment);
                    cdcMetrics.deletedSegment.metric.update(length);
                }
                catch (IOException e)
                {
                    LOGGER.warn("Failed to delete cdc segment", e);
                }
                directorySizeBytes -= length;
                i++;
            }
        }

        try
        {
            cleanupOrphanedIdxFiles(cdcRawDirectory);
        }
        catch (IOException e)
        {
            LOGGER.warn("Failed to clean up orphaned idx files", e);
        }
    }

    protected boolean validSegmentFilter(File file)
    {
        return file.isFile() && isLogFile(file.getName());
    }

    protected long deleteSegment(CdcRawSegmentFile segment) throws IOException
    {
        final long numBytes = segment.length() + segment.indexLength();
        LOGGER.info("Deleting Cdc segment path={} lastModified={} numBytes={}", segment,
                    segment.lastModified(), numBytes);
        Files.deleteIfExists(segment.path());
        Files.deleteIfExists(segment.indexPath());
        return numBytes;
    }

    // runs optionally if detects orphaned and old index files
    private void cleanupOrphanedIdxFiles(File cdcDir) throws IOException
    {
        final File[] indexFiles =
        cdcDir.listFiles(f -> f.isFile() && CdcUtil.isValidIdxFile(f.getName()));
        if (indexFiles == null || indexFiles.length == 0)
            return; // exit early when finding no index files

        final File[] cdcSegments =
        cdcDir.listFiles(f -> f.isFile() && CdcUtil.isLogFile(f.getName()));
        Set<String> cdcFileNames = Set.of();
        if (cdcSegments != null)
        {
            cdcFileNames = new HashSet<>(cdcSegments.length);
            for (File f : cdcSegments)
            {
                cdcFileNames.add(f.getName());
            }
        }

        // now, delete all old index files that have no corresponding log files.
        for (File idxFile : indexFiles)
        {
            final String cdcFileName = CdcUtil.idxToLogFileName(idxFile.getName());
            if (!cdcFileNames.contains(cdcFileName))
            {  // found an orphaned index file
                LOGGER.warn("Orphaned Cdc idx file found with no corresponding Cdc segment path={}",
                            idxFile.toPath());
                cdcMetrics.orphanedIdx.metric.update(1L);
                Files.deleteIfExists(idxFile.toPath());
            }
        }
    }

    private void publishCdcStats(@Nullable List<CdcRawSegmentFile> cdcFiles)
    {
        // no cdc data consumed or exist
        boolean noCdcFiles = cdcFiles == null || cdcFiles.isEmpty();
        if (noCdcFiles && priorCdcFiles.isEmpty())
            return;

        Map<CdcRawSegmentFile, Long> currentFiles;
        long totalCurrentBytes = 0L;
        if (noCdcFiles)
        {
            currentFiles = new HashMap<>();
        }
        else
        {
            currentFiles = new HashMap<>(cdcFiles.size());
            for (CdcRawSegmentFile segment : cdcFiles)
            {
                if (segment.exists())
                {
                    long len = segment.length();
                    currentFiles.put(segment, len);
                    totalCurrentBytes += len;
                }
            }
        }

        // skip publishing. there is no cdc data consumed and no data exist.
        if (totalCurrentBytes == 0L && priorCdcFiles.isEmpty())
        {
            priorCdcFiles = currentFiles;
            return;
        }

        // consumed files is the files exist in the prior round but now are deleted.
        Set<CdcRawSegmentFile> consumedFiles =
        Sets.difference(priorCdcFiles.keySet(), currentFiles.keySet());
        long totalConsumedBytes =
        consumedFiles.stream().map(priorCdcFiles::get).reduce(0L, Long::sum);
        priorCdcFiles.clear();
        priorCdcFiles = currentFiles;
        cdcMetrics.totalConsumedCdcBytes.metric.update(totalConsumedBytes);
        cdcMetrics.totalCdcSpaceUsed.metric.setValue(totalCurrentBytes);
    }

    /**
     * Helper class for the CdcRawDirectorySpaceCleaner to track log segment files and associated idx file in the cdc_raw directory
     */
    protected static class CdcRawSegmentFile implements Comparable<CdcRawSegmentFile>
    {
        private final File file;
        private final File indexFile;
        private final long segmentId;
        private final long len;

        CdcRawSegmentFile(File logFile)
        {
            this.file = logFile;
            final String name = logFile.getName();
            this.segmentId = parseSegmentId(name);
            this.len = logFile.length();
            this.indexFile = CdcUtil.getIdxFile(logFile);
        }

        public boolean exists()
        {
            return file.exists();
        }

        public boolean indexExists()
        {
            return indexFile.exists();
        }

        public long length()
        {
            return len;
        }

        public long indexLength()
        {
            return indexFile.length();
        }

        public long lastModified()
        {
            return file.lastModified();
        }

        public Path path()
        {
            return file.toPath();
        }

        public Path indexPath()
        {
            return indexFile.toPath();
        }

        @Override
        public int compareTo(@NotNull CdcRawSegmentFile o)
        {
            return Long.compare(segmentId, o.segmentId);
        }

        @Override
        public int hashCode()
        {
            return file.hashCode();
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
            {
                return true;
            }
            if (other == null || this.getClass() != other.getClass())
            {
                return false;
            }

            CdcRawSegmentFile that = (CdcRawSegmentFile) other;
            return file.equals(that.file);
        }

        @Override
        public String toString()
        {
            return file.getAbsolutePath();
        }
    }
}
