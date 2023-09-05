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

    @JsonProperty(value = CONCURRENT_UPLOAD_LIMIT_PROPERTY, defaultValue = DEFAULT_CONCURRENT_UPLOAD_LIMIT + "")
    protected final int concurrentUploadsLimit;

    @JsonProperty(value = MIN_FREE_SPACE_PERCENT_PROPERTY, defaultValue = DEFAULT_MIN_FREE_SPACE_PERCENT + "")
    protected final float minimumSpacePercentageRequired;

    public SSTableUploadConfigurationImpl()
    {
        this(DEFAULT_CONCURRENT_UPLOAD_LIMIT, DEFAULT_MIN_FREE_SPACE_PERCENT);
    }

    public SSTableUploadConfigurationImpl(int concurrentUploadsLimit)
    {
        this(concurrentUploadsLimit, DEFAULT_MIN_FREE_SPACE_PERCENT);
    }

    public SSTableUploadConfigurationImpl(float minimumSpacePercentageRequired)
    {
        this(DEFAULT_CONCURRENT_UPLOAD_LIMIT, minimumSpacePercentageRequired);
    }

    public SSTableUploadConfigurationImpl(int concurrentUploadsLimit,
                                          float minimumSpacePercentageRequired)
    {
        this.concurrentUploadsLimit = concurrentUploadsLimit;
        this.minimumSpacePercentageRequired = minimumSpacePercentageRequired;
    }

    /**
     * @return the maximum number of concurrent SSTable component uploads allowed for this service
     */
    @Override
    @JsonProperty(value = CONCURRENT_UPLOAD_LIMIT_PROPERTY, defaultValue = DEFAULT_CONCURRENT_UPLOAD_LIMIT + "")
    public int concurrentUploadsLimit()
    {
        return concurrentUploadsLimit;
    }

    /**
     * @return the configured minimum space percentage required for an SSTable component upload
     */
    @Override
    @JsonProperty(value = MIN_FREE_SPACE_PERCENT_PROPERTY, defaultValue = DEFAULT_MIN_FREE_SPACE_PERCENT + "")
    public float minimumSpacePercentageRequired()
    {
        return minimumSpacePercentageRequired;
    }
}
