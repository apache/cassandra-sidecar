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

package org.apache.cassandra.sidecar.cdc;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.utils.CdcUtil;

/**
 * CDCLogCache caches the recently downloaded files to avoid being deleted by accident.
 * <br>
 * Downloads tracking is via {@linkplain #touch}.
 * <br>
 * In the event of deleting the _consumed_ files, 1 supersedes 2, meaning the _consumed_ files and their links
 * are deleted, even though within the cache duration.
 */
@Singleton
public class CdcLogCache
{
    public static final String TEMP_DIR_SUFFIX = "_tmp";

    private static final Logger LOGGER = LoggerFactory.getLogger(CdcLogCache.class);
    private static final RemovalListener<File, File> hardlinkRemover = notification -> deleteFileIfExist(notification.getValue());

    private final AtomicBoolean isInitialized = new AtomicBoolean(false);
    private final TaskExecutorPool internalExecutorPool;
    private final SecondBoundConfiguration cacheExpiryConfig;

    // Cache for the hardlinks. Key: origin file; Value: link file
    // The entries expire after 5 minutes
    @VisibleForTesting
    final Cache<File, File> hardlinkCache;

    @Inject
    public CdcLogCache(ExecutorPools executorPools,
                       InstancesMetadata instancesMetadata,
                       SidecarConfiguration sidecarConfig)
    {
        this(executorPools, instancesMetadata, sidecarConfig.serviceConfiguration().cdcConfiguration().segmentHardLinkCacheExpiry());
    }

    @VisibleForTesting
    CdcLogCache(ExecutorPools executorPools,
                InstancesMetadata instancesMetadata,
                SecondBoundConfiguration cacheExpiryConfig)
    {
        this.cacheExpiryConfig = cacheExpiryConfig;
        this.internalExecutorPool = executorPools.internal();
        this.hardlinkCache = CacheBuilder.newBuilder()
                                         .expireAfterAccess(cacheExpiryConfig.quantity(), cacheExpiryConfig.unit())
                                         .removalListener(hardlinkRemover)
                                         .build();
        // Run cleanup in the internal pool to mute any exceptions. The cleanup is best-effort.
        internalExecutorPool.runBlocking(() -> cleanupLinkedFilesOnStartup(instancesMetadata));
    }

    public void initMaybe()
    {
        if (isInitialized.get())
        {
            return;
        }

        if (isInitialized.compareAndSet(false, true))
        {
            // setup periodic and serial cleanup
            long cacheExpiryInMillis = cacheExpiryConfig.to(TimeUnit.MILLISECONDS);
            internalExecutorPool.setPeriodic(cacheExpiryInMillis,
                                             id -> hardlinkCache.cleanUp(),
                                             true);
        }
    }

    public void touch(File segmentFile, File indexFile)
    {
        // renew the hardlinks
        hardlinkCache.getIfPresent(segmentFile);
        hardlinkCache.getIfPresent(indexFile);
    }

    /**
     * Create a hardlink from origin in the cache if the hardlink does not exist yet.
     *
     * @param origin the source file
     * @return the link file
     * @throws IOException when an IO exception occurs during link
     */
    public File createLinkedFileInCache(File origin) throws IOException
    {
        File link = hardlinkCache.getIfPresent(origin);
        if (link == null)
        {
            link = new File(getTempCdcDir(origin.getParent()), origin.getName());
            try
            {
                // create link and cache it
                Files.createLink(link.toPath(), origin.toPath());
                hardlinkCache.put(origin, link);
            }
            catch (FileAlreadyExistsException e)
            {
                LOGGER.debug("The target of hardlink {} already exists. It could be created by a concurrent request.", link);
            }
        }
        return link;
    }

    /**
     * Clean up the linked file when the application is starting.
     * There could be files left over if the application crashes during streaming the CDC segments.
     * On a new start, the tmp directory for the linked CDC segments should be empty.
     * It is only called in the constructor of the handler singleton.
     *
     * @param config instances config
     */
    @VisibleForTesting
    public void cleanupLinkedFilesOnStartup(InstancesMetadata config)
    {
        for (InstanceMetadata instance : config.instances())
        {
            try
            {
                cleanupLinkedFiles(instance);
            }
            catch (Exception e)
            {
                LOGGER.warn("Failed to clean up linked files for instance {}", instance.id(), e);
            }
        }
    }

    private void cleanupLinkedFiles(InstanceMetadata instance) throws IOException
    {
        File[] files = getTempCdcDir((instance.cdcDir()))
                       .listFiles(f -> CdcUtil.isLogFile(f.getName()) || CdcUtil.isIndexFile(f.getName()));
        if (files == null)
            return;
        for (File f : files)
        {
            deleteFileIfExist(f);
        }
    }

    private static void deleteFileIfExist(File file)
    {
        if (file.exists() && file.isFile())
        {
            if (file.delete())
            {
                LOGGER.debug("Removed the link file={}", file);
            }
        }
    }

    private static File getTempCdcDir(String cdcDir) throws IOException
    {
        File dir = new File(cdcDir + TEMP_DIR_SUFFIX);
        try
        {
            Files.createDirectories(dir.toPath());
        }
        catch (IOException e)
        {
            LOGGER.error("Unable to create temporary CDC directory {}", dir, e);
            throw e;
        }
        return dir;
    }
}
