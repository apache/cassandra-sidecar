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

package org.apache.cassandra.sidecar.config.yaml;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.file.FileSystemOptions;
import org.apache.cassandra.sidecar.config.VertxFilesystemOptionsConfiguration;

/**
 * Encapsulates configuration needed for vert.x {@link io.vertx.core.file.FileSystemOptions}
 */
public class VertxFilesystemOptionsConfigurationImpl implements VertxFilesystemOptionsConfiguration
{
    @JsonProperty(value = "class_path_resolving_enabled")
    private final boolean classPathResolvingEnabled;

    private String fileCacheDir;

    @JsonProperty(value = "file_caching_enabled")
    private final boolean fileCachingEnabled;

    public static final boolean DEFAULT_CLASSPATH_RESOLVING_ENABLED = false;
    public static final boolean DEFAULT_FILE_CACHING_ENABLED = false;

    public VertxFilesystemOptionsConfigurationImpl()
    {
        this(DEFAULT_CLASSPATH_RESOLVING_ENABLED,
             FileSystemOptions.DEFAULT_FILE_CACHING_DIR,
             DEFAULT_FILE_CACHING_ENABLED);
    }

    public VertxFilesystemOptionsConfigurationImpl(boolean classPathResolvingEnabled,
                                                   String fileCacheDir,
                                                   boolean fileCachingEnabled)
    {

        this.classPathResolvingEnabled = classPathResolvingEnabled;
        this.fileCachingEnabled = fileCachingEnabled;
        setFileCacheDir(fileCacheDir);
    }

    @Override
    @JsonProperty(value = "class_path_resolving_enabled")
    public boolean classPathResolvingEnabled()
    {
        return classPathResolvingEnabled;
    }

    @Override
    @JsonProperty(value = "file_cache_dir")
    public String fileCacheDir()
    {
        return fileCacheDir;
    }

    @JsonProperty(value = "file_cache_dir")
    public void setFileCacheDir(String fileCacheDir)
    {
        if (fileCacheDir == null || fileCacheDir.isEmpty())
        {
            // Honor vert.x's default configuration when the fileCacheDir
            // is not configured
            this.fileCacheDir = FileSystemOptions.DEFAULT_FILE_CACHING_DIR;
        }
        else
        {
            this.fileCacheDir = fileCacheDir;
        }
    }

    @Override
    @JsonProperty(value = "file_caching_enabled")
    public boolean fileCachingEnabled()
    {
        return fileCachingEnabled;
    }
}
