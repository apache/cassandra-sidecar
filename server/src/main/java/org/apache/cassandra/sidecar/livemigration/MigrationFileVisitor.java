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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jetbrains.annotations.NotNull;

/**
 * File visitor to walk through given directory.
 */
public class MigrationFileVisitor extends SimpleFileVisitor<Path>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MigrationFileVisitor.class);
    private final String homeDir;
    private final List<Path> validFiles;
    private final List<PathMatcher> filesToExclude;
    private final List<PathMatcher> directoriesToExclude;

    public MigrationFileVisitor(@NotNull String homeDir,
                                @NotNull List<PathMatcher> filesToExcludeMatchers,
                                @NotNull List<PathMatcher> directoriesToExcludeMatchers)
    {
        this.homeDir = homeDir;
        this.validFiles = new ArrayList<>();
        this.filesToExclude = filesToExcludeMatchers;
        this.directoriesToExclude = directoriesToExcludeMatchers;
    }

    private boolean shouldExcludeDir(Path dir)
    {
        Objects.requireNonNull(dir);
        return directoriesToExclude.stream().anyMatch(matcher -> matcher.matches(dir));
    }

    private boolean shouldExcludeFile(Path file)
    {
        Objects.requireNonNull(file);
        return filesToExclude.stream().anyMatch(matcher -> matcher.matches(file));
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
    {
        if (shouldExcludeDir(dir))
        {
            return FileVisitResult.SKIP_SUBTREE;
        }

        if (!dir.toAbsolutePath().toString().equals(homeDir))
        {
            validFiles.add(dir);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
    {
        Objects.requireNonNull(file);
        Objects.requireNonNull(attrs);

        if (!shouldExcludeFile(file))
        {
            validFiles.add(file);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException
    {
        // SimpleFileVisitor is checking if the file is null. Hence, checking the same condition here.
        Objects.requireNonNull(file);
        boolean isDirectory = Files.isDirectory(file);
        final String absolutePath = file.toFile().getAbsolutePath();
        if ((isDirectory && shouldExcludeDir(file)) || (!isDirectory && shouldExcludeFile(file)))
        {
            LOGGER.info("Got the exception wile trying to visit: {}. However, it's a part of the exclude list. " +
                        "Hence ignoring the exception.", absolutePath, exc);
            return FileVisitResult.CONTINUE;
        }
        else
        {
            LOGGER.error("File/Directory visit failed for: {}", absolutePath, exc);
            throw exc;
        }
    }

    public List<Path> validFilePaths()
    {
        return validFiles;
    }
}
