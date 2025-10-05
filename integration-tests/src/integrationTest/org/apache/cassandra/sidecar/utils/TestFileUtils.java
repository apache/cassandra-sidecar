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

package org.apache.cassandra.sidecar.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for handling files on integration tests
 */
public class TestFileUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestFileUtils.class);

    public static void extractGzippedTarball(Path tarballPath, Path targetDir) throws IOException
    {
        // check if tarball exists and ends with .tar.gz
        if (!tarballPath.toFile().exists() || !tarballPath.toString().endsWith(".tar.gz"))
        {
            throw new IllegalStateException("Cassandra tarball does not exist or is not a .tar.gz file: " + tarballPath);
        }
        LOGGER.info("Extracting tarball {} to directory {}", tarballPath, targetDir);
        try (TarArchiveInputStream tarInput = new TarArchiveInputStream(new GzipCompressorInputStream(Files.newInputStream(tarballPath))))
        {
            TarArchiveEntry entry;
            while ((entry = tarInput.getNextEntry()) != null)
            {
                Path entryPath = targetDir.resolve(entry.getName());
                if (entry.isDirectory())
                {
                    Files.createDirectories(entryPath);
                }
                else
                {
                    Files.copy(tarInput, entryPath);
                    // Set correct permission from TarArchiveEntry
                    if (entry.getMode() != 0)
                    {
                        Files.setPosixFilePermissions(entryPath, permissionsFromInteger(entry.getMode()));
                    }
                }
            }
        }
        LOGGER.info("Extracted tarball {} to directory {}", tarballPath, targetDir);
    }

    public static void copyDirectoryRecursively(Path sourceDir, Path targetDir) throws IOException
    {
        LOGGER.info("Copying directory {} to {}", sourceDir, targetDir);
        try (var walk = Files.walk(sourceDir))
        {
            walk.forEach(source ->
            {
                try
                {
                    Path target = targetDir.resolve(sourceDir.relativize(source));
                    if (Files.isDirectory(source))
                    {
                        Files.createDirectories(target);
                    }
                    else
                    {
                        Files.createDirectories(target.getParent());
                        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                }
                catch (IOException e)
                {
                    throw new RuntimeException("Failed to copy " + source, e);
                }
            });
        }
    }

    public static void replacePlaceholdersInFileWithPattern(Path templateFile, Map<String, String> placeholders,
                                                            Path destFile) throws IOException
    {
        LOGGER.info("Replacing placeholders {} in file {}", placeholders.keySet(), templateFile);
        String content = Files.readString(templateFile);
        for (Map.Entry<String, String> entry : placeholders.entrySet())
        {
            String placeholder = "\\$\\{" + Pattern.quote(entry.getKey()) + "\\}";
            content = content.replaceAll(placeholder, Matcher.quoteReplacement(entry.getValue()));
        }
        Files.writeString(destFile, content, StandardOpenOption.CREATE_NEW);
        LOGGER.info("Replaced {} placeholders in file {} and writing to {}", placeholders.size(), templateFile, destFile);
    }

    public static Set<PosixFilePermission> permissionsFromInteger(int mode)
    {
        Set<PosixFilePermission> permissions = EnumSet.noneOf(PosixFilePermission.class);

        // Owner permissions
        if ((mode & 0400) != 0)
        {
            permissions.add(PosixFilePermission.OWNER_READ);
        }
        if ((mode & 0200) != 0)
        {
            permissions.add(PosixFilePermission.OWNER_WRITE);
        }
        if ((mode & 0100) != 0)
        {
            permissions.add(PosixFilePermission.OWNER_EXECUTE);
        }

        // Group permissions
        if ((mode & 0040) != 0)
        {
            permissions.add(PosixFilePermission.GROUP_READ);
        }
        if ((mode & 0020) != 0)
        {
            permissions.add(PosixFilePermission.GROUP_WRITE);
        }
        if ((mode & 0010) != 0)
        {
            permissions.add(PosixFilePermission.GROUP_EXECUTE);
        }

        // Others permissions
        if ((mode & 0004) != 0)
        {
            permissions.add(PosixFilePermission.OTHERS_READ);
        }
        if ((mode & 0002) != 0)
        {
            permissions.add(PosixFilePermission.OTHERS_WRITE);
        }
        if ((mode & 0001) != 0)
        {
            permissions.add(PosixFilePermission.OTHERS_EXECUTE);
        }

        return permissions;
    }
}
