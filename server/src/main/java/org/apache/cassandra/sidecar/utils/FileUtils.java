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

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetbrains.annotations.Nullable;

/**
 * Encompasses utilities for files
 */
public class FileUtils
{
    private static final Pattern STORAGE_UNIT_PATTERN = Pattern.compile("(\\d+)(GiB|MiB|KiB|B)?");

    /**
     * Resolves the home directory from the input {@code directory} string when the string begins with {@code ~}.
     *
     * @param directory the directory path
     * @return the resolved directory path, replacing the user's home directory when the string begins with {@code ~}
     */
    public static String maybeResolveHomeDirectory(String directory)
    {
        if (directory == null || !directory.startsWith("~"))
            return directory;
        return System.getProperty("user.home") + directory.substring(1);
    }

    /**
     * @param directory the directory
     * @return the size in bytes of all files in a directory, non-recursively.
     */
    public static long directorySize(File directory)
    {
        long size = 0;
        final File[] files = directory.listFiles();
        if (files != null)
        {
            for (File file : files)
            {
                if (file.isFile())
                {
                    size += file.length();
                }
            }
        }
        return size;
    }

    public static long mbStringToBytes(String str)
    {
        return Long.parseLong(str) * (1 << 20);  // note the prop name uses 'mb' but Cassandra parses as MiB
    }

    @Nullable
    public static Long storageStringToBytes(String str)
    {
        final Matcher matcher = STORAGE_UNIT_PATTERN.matcher(str);
        if (matcher.find())
        {
            return Long.parseLong(matcher.group(1)) * storageUnitToBytes(matcher.group(2));
        }
        return null;
    }

    public static long storageUnitToBytes(String unit)
    {
        if (unit == null)
        {
            return 1;
        }

        switch (unit)
        {
            case "GiB":
                return 1 << 30;
            case "MiB":
                return 1 << 20;
            case "KiB":
                return 1024;
            case "":
            case "B":
                return 1;
        }
        throw new IllegalStateException("Unexpected data storage unit: " + unit);
    }
}
