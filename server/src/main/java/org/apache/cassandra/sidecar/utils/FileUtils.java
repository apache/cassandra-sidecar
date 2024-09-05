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

/**
 * Encompasses utilities for files
 */
public class FileUtils
{
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
}
