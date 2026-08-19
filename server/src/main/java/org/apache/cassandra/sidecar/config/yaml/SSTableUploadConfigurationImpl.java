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

import java.nio.file.attribute.PosixFilePermissions;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.SSTableUploadConfiguration;

/**
 * Configuration for SSTable component uploads on this service
 */
public class SSTableUploadConfigurationImpl implements SSTableUploadConfiguration
{
    public static final String CONCURRENT_UPLOAD_LIMIT_PROPERTY = "concurrent_upload_limit";
    public static final int DEFAULT_CONCURRENT_UPLOAD_LIMIT = 80;

    public static final String MIN_FREE_SPACE_PERCENT_PROPERTY = "min_free_space_percent";
    public static final float DEFAULT_MIN_FREE_SPACE_PERCENT = 10;

    public static final String FILE_PERMISSIONS_PROPERTY = "file_permissions";
    public static final String DEFAULT_FILE_PERMISSIONS = "rw-r--r--";

    public static final String RETRY_AFTER_SECONDS_PROPERTY = "retry_after_seconds";
    public static final int DEFAULT_RETRY_AFTER_SECONDS = 1;

    @JsonProperty(value = CONCURRENT_UPLOAD_LIMIT_PROPERTY)
    protected final int concurrentUploadsLimit;

    @JsonProperty(value = MIN_FREE_SPACE_PERCENT_PROPERTY)
    protected final float minimumSpacePercentageRequired;

    protected String filePermissions;

    @JsonProperty(value = RETRY_AFTER_SECONDS_PROPERTY)
    protected final int retryAfterSeconds;

    public SSTableUploadConfigurationImpl()
    {
        this(DEFAULT_CONCURRENT_UPLOAD_LIMIT, DEFAULT_MIN_FREE_SPACE_PERCENT, DEFAULT_FILE_PERMISSIONS,
             DEFAULT_RETRY_AFTER_SECONDS);
    }

    public SSTableUploadConfigurationImpl(int concurrentUploadsLimit)
    {
        this(concurrentUploadsLimit, DEFAULT_MIN_FREE_SPACE_PERCENT, DEFAULT_FILE_PERMISSIONS,
             DEFAULT_RETRY_AFTER_SECONDS);
    }

    public SSTableUploadConfigurationImpl(float minimumSpacePercentageRequired)
    {
        this(DEFAULT_CONCURRENT_UPLOAD_LIMIT, minimumSpacePercentageRequired, DEFAULT_FILE_PERMISSIONS,
             DEFAULT_RETRY_AFTER_SECONDS);
    }

    public SSTableUploadConfigurationImpl(String filePermissions)
    {
        this(DEFAULT_CONCURRENT_UPLOAD_LIMIT, DEFAULT_MIN_FREE_SPACE_PERCENT, filePermissions,
             DEFAULT_RETRY_AFTER_SECONDS);
    }

    public SSTableUploadConfigurationImpl(int concurrentUploadsLimit,
                                          float minimumSpacePercentageRequired,
                                          String filePermissions,
                                          int retryAfterSeconds)
    {
        this.concurrentUploadsLimit = concurrentUploadsLimit;
        this.minimumSpacePercentageRequired = minimumSpacePercentageRequired;
        this.retryAfterSeconds = retryAfterSeconds;
        setFilePermissions(filePermissions);
    }

    /**
     * @deprecated use {@link #SSTableUploadConfigurationImpl(int, float, String, int)} instead; kept for
     * backwards compatibility with callers built against the pre-{@code retryAfterSeconds} constructor
     */
    @Deprecated
    public SSTableUploadConfigurationImpl(int concurrentUploadsLimit,
                                          float minimumSpacePercentageRequired,
                                          String filePermissions)
    {
        this(concurrentUploadsLimit, minimumSpacePercentageRequired, filePermissions, DEFAULT_RETRY_AFTER_SECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = CONCURRENT_UPLOAD_LIMIT_PROPERTY)
    public int concurrentUploadsLimit()
    {
        return concurrentUploadsLimit;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = MIN_FREE_SPACE_PERCENT_PROPERTY)
    public float minimumSpacePercentageRequired()
    {
        return minimumSpacePercentageRequired;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = FILE_PERMISSIONS_PROPERTY, defaultValue = DEFAULT_FILE_PERMISSIONS)
    public String filePermissions()
    {
        return filePermissions;
    }

    @JsonProperty(value = FILE_PERMISSIONS_PROPERTY, defaultValue = DEFAULT_FILE_PERMISSIONS)
    public void setFilePermissions(String filePermissions)
    {
        if (filePermissions != null)
        {
            try
            {
                // forces a validation of the input
                this.filePermissions = PosixFilePermissions.toString(PosixFilePermissions.fromString(filePermissions));
            }
            catch (IllegalArgumentException exception)
            {
                String errorMessage = String.format("Invalid file_permissions configuration=\"%s\"", filePermissions);
                throw new IllegalArgumentException(errorMessage);
            }
        }
        else
        {
            this.filePermissions = null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = RETRY_AFTER_SECONDS_PROPERTY)
    public int retryAfterSeconds()
    {
        return retryAfterSeconds;
    }
}
