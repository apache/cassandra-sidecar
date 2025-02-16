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
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.ExecutorPoolsHelper;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.server.MainModule;

import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CdcLogCacheTest
{
    private final Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
    private final InstancesMetadata instancesMetadata = injector.getInstance(InstancesMetadata.class);
    private final Vertx vertx = injector.getInstance(Vertx.class);
    private final ExecutorPools executorPools = injector.getInstance(ExecutorPools.class);
    private final CdcLogCache logCache = cdcLogCache(executorPools);

    @BeforeEach
    void beforeEach()
    {
        logCache.initMaybe();
        logCache.hardlinkCache.invalidateAll();
        assertThat(logCache.hardlinkCache.size()).isZero();
    }

    @AfterEach
    void teardown()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testLinkedFileExpiryInCache() throws IOException
    {
        File commitLogFile = instance1CommitLogFile();
        File linkedCommitLog = logCache.createLinkedFileInCache(commitLogFile);
        assertThat(linkedCommitLog).isNotNull();
        assertThat(logCache.hardlinkCache.size()).isOne();

        // wait for file to expire
        loopAssert(2, () -> assertThat(logCache.hardlinkCache.size()).isZero());
    }

    @Test
    void testCleanUpLinkedFiles() throws IOException
    {
        File commitLogFile = instance1CommitLogFile();
        File linkedFile = logCache.createLinkedFileInCache(commitLogFile);
        assertThat(linkedFile.exists()).isTrue();

        // Verify that cleanup deletes the linked file
        logCache.cleanupLinkedFilesOnStartup(instancesMetadata);
        assertThat(linkedFile.exists()).isFalse();
    }

    @Test
    void testCreateLinkedFileInCache() throws IOException
    {
        File commitLogFile = instance1CommitLogFile();
        File linkedCommitLog = logCache.createLinkedFileInCache(commitLogFile);

        // Check if hard link is created
        assertThat(commitLogFile).isNotEqualTo(linkedCommitLog);
        assertThat(Files.isSameFile(commitLogFile.toPath(), linkedCommitLog.toPath())).isTrue();

        // Should return cached linked file in subsequent calls
        assertThat(linkedCommitLog).isEqualTo(logCache.createLinkedFileInCache(commitLogFile));
    }

    /**
     * Failing to clean up shouldn't fail to initialize the class
     */
    @Test
    void testCleanupErrorDoesntPreventInitialization()
    {
        assertThatNoException().isThrownBy(() -> {
            new FailingCdcLogCache(ExecutorPoolsHelper.createdSharedTestPool(Vertx.vertx()), instancesMetadata, sidecarConfiguration());
        });
    }

    private File instance1CommitLogFile()
    {
        String commitLogPathOnInstance1 = instancesMetadata.instances().get(0).cdcDir() + "/CommitLog-1-1.log";
        return new File(commitLogPathOnInstance1);
    }

    private CdcLogCache cdcLogCache(ExecutorPools executorPools)
    {
        // Mock the class because even though the resolution is seconds, for testing purposes
        // we hack into the class and allow configuring the cache expiration with milliseconds.
        SecondBoundConfiguration mockCacheExpiryConfig = mock(SecondBoundConfiguration.class);
        when(mockCacheExpiryConfig.quantity()).thenReturn(100L);
        when(mockCacheExpiryConfig.unit()).thenReturn(TimeUnit.MILLISECONDS);
        when(mockCacheExpiryConfig.to(TimeUnit.MILLISECONDS)).thenCallRealMethod();
        return new CdcLogCache(executorPools, instancesMetadata, mockCacheExpiryConfig);
    }

    private SidecarConfiguration sidecarConfiguration()
    {
        SidecarConfiguration sidecarConfiguration = mock(SidecarConfiguration.class, RETURNS_DEEP_STUBS);
        when(sidecarConfiguration.serviceConfiguration().cdcConfiguration().segmentHardLinkCacheExpiry())
        .thenReturn(SecondBoundConfiguration.parse("1s"));
        return sidecarConfiguration;
    }

    static class FailingCdcLogCache extends CdcLogCache
    {
        public FailingCdcLogCache(ExecutorPools executorPools, InstancesMetadata cassandraConfig, SidecarConfiguration sidecarConfig)
        {
            super(executorPools, cassandraConfig, sidecarConfig);
        }

        @Override
        public void cleanupLinkedFilesOnStartup(InstancesMetadata config)
        {
            // Fake an error to simulate the initialization issue
            Preconditions.checkState(false, "cdc_raw_tmp should be a directory");
        }
    }
}
