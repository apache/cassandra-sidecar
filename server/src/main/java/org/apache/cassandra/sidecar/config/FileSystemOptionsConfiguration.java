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

package org.apache.cassandra.sidecar.config;

import io.vertx.core.file.FileSystemOptions;

/**
 * Exposes configuration in Sidecar for vert.x {@link FileSystemOptions}
 */
public interface FileSystemOptionsConfiguration
{
    /**
     * When vert.x cannot find the file on the filesystem it tries to resolve the
     * file from the classpath when this is set to {@code true}. Otherwise, vert.x
     * will not attempt to resolve the file on the classpath
     *
     * @return {@code true} if classpath resolving is enabled, {@code false} otherwise.
     */
    boolean classpathResolvingEnabled();

    /**
     * When vert.x reads a file that is packaged with the application it gets
     * extracted to this directory first and subsequent reads will use the extracted
     * file to get better IO performance.
     *
     * @return the configured file cache dir
     */
    String fileCacheDir();

    /**
     * Returns {@code true} to enable caching files on the real file system
     * when the filesystem performs class path resolving. {@code false} to
     * disable caching.
     *
     * @return {@code true} when caching files on the underlying file system is enabled
     * {@code false} otherwise
     */
    boolean fileCachingEnabled();
}
