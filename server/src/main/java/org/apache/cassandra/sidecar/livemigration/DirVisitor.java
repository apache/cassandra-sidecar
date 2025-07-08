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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.response.InstanceFileInfo;
import org.jetbrains.annotations.NotNull;

/**
 * Explores single directory of a Cassandra Instance.
 */
public class DirVisitor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DirVisitor.class);
    private final String homeDir;
    final Path homeDirPath;
    private final String pathPrefix;
    final MigrationFileVisitor fileVisitor;

    public DirVisitor(String homeDir,
                      String pathPrefix,
                      List<PathMatcher> fileExclusionMatchers,
                      List<PathMatcher> dirExclusionMatchers)
    {
        this.homeDir = homeDir;
        this.homeDirPath = Paths.get(homeDir);
        this.pathPrefix = pathPrefix;
        this.fileVisitor = new MigrationFileVisitor(homeDir, fileExclusionMatchers, dirExclusionMatchers);
    }

    /**
     * Returns list of files and directories (urls) that can be downloaded by destination which is trying
     * to clone current instance.
     *
     * @return List of {@link InstanceFileInfo} for each file and directory considered for Live Migration.
     * @throws IOException - when cannot access files of a data home directory
     */
    public List<InstanceFileInfo> files() throws IOException
    {
        Files.walkFileTree(homeDirPath, fileVisitor);
        List<Path> validFiles = fileVisitor.validFilePaths();
        final List<InstanceFileInfo> infos = new ArrayList<>();
        for (final Path path : validFiles)
        {
            infos.add(toInstanceFileInfo(path));
        }
        LOGGER.debug("Found {} instance files for homeDir {}", infos.size(), homeDir);
        return infos;
    }

    private InstanceFileInfo toInstanceFileInfo(@NotNull Path path) throws IOException
    {
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
        long lastModifiedTime = attrs.lastModifiedTime().toMillis();
        String fileUrl = buildInstanceFileUrl(path);
        boolean isDirectory = attrs.isDirectory();
        InstanceFileInfo.FileType fileType = isDirectory
                                             ? InstanceFileInfo.FileType.DIRECTORY
                                             : InstanceFileInfo.FileType.FILE;
        // 'size' doesn't have any significance for directories. Hence, setting it to -1 explicitly.
        long size = isDirectory ? -1 : attrs.size();

        return new InstanceFileInfo(fileUrl, size, fileType, lastModifiedTime);
    }

    /**
     * Constructs live migration URL path for given file.
     *
     * @param path file for which URL needs to be constructed.
     * @return URL path for given file
     */
    private String buildInstanceFileUrl(@NotNull Path path)
    {
        String relativePath = homeDirPath.relativize(path).toString();
        return pathPrefix + "/" + relativePath;
    }
}
