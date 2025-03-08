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

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_CDC_RAW_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_COMMITLOG_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_HINTS_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_SAVED_CACHES_DIR_PATH;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.CDC_RAW_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.COMMITLOG_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.DATA_FILE_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.HINTS_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.LOCAL_SYSTEM_DATA_FILE_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.SAVED_CACHES_DIR_PLACEHOLDER;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.hasAnyPlaceholder;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.hasPlaceholder;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.replacePlaceholder;

/**
 * Helper class to get the list of files to use during Live Migration.
 */
public class CassandraInstanceFilesImpl implements CassandraInstanceFiles
{

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraInstanceFilesImpl.class);

    private final InstanceMetadata instanceMetadata;

    private final LiveMigrationConfiguration configuration;

    public CassandraInstanceFilesImpl(InstanceMetadata instanceMetadata,
                                      LiveMigrationConfiguration configuration)
    {
        this.instanceMetadata = instanceMetadata;
        this.configuration = configuration;
    }

    @Override
    public List<InstanceFileInfo> getFiles() throws IOException
    {
        return getFiles(configuration.filesToExclude(), configuration.directoriesToExclude());
    }

    private List<InstanceFileInfo> getFiles(Set<String> filesToExclude,
                                            Set<String> dirsToExclude) throws IOException
    {
        List<DirVisitor> dirVisitors = getDirVisitorList(filesToExclude, dirsToExclude);
        List<InstanceFileInfo> instanceFileInfos = new ArrayList<>();

        for (DirVisitor dirVisitor : dirVisitors)
        {
            instanceFileInfos.addAll(dirVisitor.getFiles());
        }

        LOGGER.info("{} number of files are getting listed", instanceFileInfos.size());
        return instanceFileInfos;
    }

    public List<DirVisitor> getDirVisitorList(Set<String> filesToExclude,
                                              Set<String> dirsToExclude)
    {

        List<DirVisitor> dataFilesToVisit = new ArrayList<>();

        getDirToVisit(instanceMetadata.hintsDir(),
                      0,
                      Collections.singleton(HINTS_DIR_PLACEHOLDER),
                      LIVE_MIGRATION_HINTS_DIR_PATH,
                      filesToExclude,
                      dirsToExclude)
        .ifPresent(dataFilesToVisit::add);


        getDirToVisit(instanceMetadata.commitlogDir(),
                      0,
                      Collections.singleton(COMMITLOG_DIR_PLACEHOLDER),
                      LIVE_MIGRATION_COMMITLOG_DIR_PATH,
                      filesToExclude,
                      dirsToExclude).ifPresent(dataFilesToVisit::add);


        getDirToVisit(instanceMetadata.savedCachesDir(),
                      0,
                      Collections.singleton(SAVED_CACHES_DIR_PLACEHOLDER),
                      LIVE_MIGRATION_SAVED_CACHES_DIR_PATH,
                      filesToExclude,
                      dirsToExclude).ifPresent(dataFilesToVisit::add);


        getDirToVisit(instanceMetadata.cdcDir(),
                      0,
                      Collections.singleton(CDC_RAW_DIR_PLACEHOLDER),
                      LIVE_MIGRATION_CDC_RAW_DIR_PATH,
                      filesToExclude,
                      dirsToExclude).ifPresent(dataFilesToVisit::add);


        getDirToVisit(instanceMetadata.localSystemDataFileDir(),
                      0,
                      Collections.singleton(LOCAL_SYSTEM_DATA_FILE_DIR_PLACEHOLDER),
                      LIVE_MIGRATION_LOCAL_SYSTEM_DATA_FILE_DIR_PATH,
                      filesToExclude,
                      dirsToExclude).ifPresent(dataFilesToVisit::add);

        List<String> dataDirs = instanceMetadata.dataDirs();
        for (int i = 0; i < dataDirs.size(); i++)
        {
            String dataDir = dataDirs.get(i);
            Set<String> dataDirPlaceHolders = new java.util.HashSet<>();
            dataDirPlaceHolders.add(DATA_FILE_DIR_PLACEHOLDER);
            dataDirPlaceHolders.add(DATA_FILE_DIR_PLACEHOLDER + "_" + i);

            getDirToVisit(dataDir,
                          i,
                          dataDirPlaceHolders,
                          LIVE_MIGRATION_DATA_FILE_DIR_PATH,
                          filesToExclude,
                          dirsToExclude).ifPresent(dataFilesToVisit::add);
        }

        return dataFilesToVisit;
    }

    Optional<DirVisitor> getDirToVisit(String homeDir, int index, Set<String> placeholders, String pathPrefix,
                                       Set<String> filesToExclude, Set<String> dirsToExclude)
    {
        if (null == homeDir)
        {
            return Optional.empty();
        }
        Path homeDirPath = Paths.get(homeDir);
        if (!Files.exists(homeDirPath))
        {
            return Optional.empty();
        }

        Set<PathMatcher> fileExclusionMatchers = toPathMatchers(filesToExclude, placeholders, homeDir);
        Set<PathMatcher> dirExclusionMatchers = toPathMatchers(dirsToExclude, placeholders, homeDir);

        return Optional.of(new DirVisitor(homeDir, index, pathPrefix, fileExclusionMatchers, dirExclusionMatchers));
    }

    Set<PathMatcher> toPathMatchers(Set<String> exclusions, Set<String> homeDirPlaceholders, String homeDir)
    {
        if (null == exclusions)
        {
            return Collections.emptySet();
        }

        return exclusions.stream()
                         .filter(exclusion -> !hasAnyPlaceholder(exclusion)
                                              || hasPlaceholder(exclusion, homeDirPlaceholders))
                         .map(file -> replacePlaceholder(file, homeDirPlaceholders, homeDir))
                         .filter(exclusion -> exclusion != null)
                         .map(file -> FileSystems.getDefault().getPathMatcher(file))
                         .collect(Collectors.toSet());
    }
}
