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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;

import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.hasAnyPlaceholder;
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
    public List<InstanceFileInfo> files() throws IOException
    {
        return files(configuration.filesToExclude(), configuration.directoriesToExclude());
    }

    private List<InstanceFileInfo> files(Set<String> filesToExclude,
                                         Set<String> dirsToExclude) throws IOException
    {
        List<DirVisitor> dirVisitors = dirVisitorList(filesToExclude, dirsToExclude);
        List<InstanceFileInfo> instanceFileInfos = new ArrayList<>();

        for (DirVisitor dirVisitor : dirVisitors)
        {
            instanceFileInfos.addAll(dirVisitor.files());
        }

        LOGGER.info("{} number of files are getting listed", instanceFileInfos.size());
        return instanceFileInfos;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<DirVisitor> dirVisitorList()
    {
        return dirVisitorList(configuration.filesToExclude(), configuration.directoriesToExclude());
    }

    private List<DirVisitor> dirVisitorList(Set<String> filesToExclude,
                                            Set<String> dirsToExclude)
    {
        List<DirVisitor> dataFilesToVisit = new ArrayList<>();
        List<String> dirsToCopy = LiveMigrationInstanceMetadataUtil.dirsToCopy(instanceMetadata);
        Map<String, String> dirPathPrefix = LiveMigrationInstanceMetadataUtil.dirPathPrefixMap(instanceMetadata);
        Map<String, Set<String>> dirPlaceholderMap = LiveMigrationInstanceMetadataUtil.dirPlaceHoldersMap(instanceMetadata);

        for (String dir : dirsToCopy)
        {
            dirToVisit(dir,
                       dirPlaceholderMap.get(dir),
                       dirPathPrefix.get(dir),
                       filesToExclude,
                       dirsToExclude)
            .ifPresent(dataFilesToVisit::add);
        }

        return dataFilesToVisit;
    }

    private Optional<DirVisitor> dirToVisit(String homeDir, Set<String> placeholders, String pathPrefix,
                                            Set<String> filesToExclude, Set<String> dirsToExclude)
    {
        if (null == homeDir)
        {
            return Optional.empty();
        }

        Objects.requireNonNull(placeholders, "placeholders are required");
        Objects.requireNonNull(pathPrefix, "pathPrefix is required");

        Path homeDirPath = Paths.get(homeDir);
        if (!Files.exists(homeDirPath))
        {
            LOGGER.info("Directory {} does not exist.", homeDir);
            return Optional.empty();
        }

        List<PathMatcher> fileExclusionMatchers = toPathMatchers(filesToExclude, placeholders, homeDir);
        List<PathMatcher> dirExclusionMatchers = toPathMatchers(dirsToExclude, placeholders, homeDir);

        return Optional.of(new DirVisitor(homeDir, pathPrefix, fileExclusionMatchers, dirExclusionMatchers));
    }

    List<PathMatcher> toPathMatchers(Set<String> exclusions, Set<String> homeDirPlaceholders, String homeDir)
    {
        if (null == exclusions || exclusions.isEmpty())
        {
            return Collections.emptyList();
        }

        return exclusions.stream()
                         .filter(exclusion -> !hasAnyPlaceholder(exclusion)
                                              || hasAnyPlaceholder(exclusion, homeDirPlaceholders))
                         .map(file -> replacePlaceholder(file, homeDirPlaceholders, homeDir))
                         .filter(Objects::nonNull)
                         .map(file -> FileSystems.getDefault().getPathMatcher(file))
                         .collect(Collectors.toList());
    }
}
