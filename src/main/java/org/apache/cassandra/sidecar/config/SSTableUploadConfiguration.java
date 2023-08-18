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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Configuration for SSTable component uploads on this service
 */
public class SSTableUploadConfiguration
{
    public static final String CONCURRENT_UPLOAD_LIMIT_PROPERTY = "concurrent_upload_limit";
    public static final int DEFAULT_CONCURRENT_UPLOAD_LIMIT = 80;
    public static final String MIN_FREE_SPACE_PERCENT_PROPERTY = "min_free_space_percent";
    public static final float DEFAULT_MIN_FREE_SPACE_PERCENT = 10;


    @JsonProperty(value = CONCURRENT_UPLOAD_LIMIT_PROPERTY, defaultValue = DEFAULT_CONCURRENT_UPLOAD_LIMIT + "")
    private final int concurrentUploadsLimit;

    @JsonProperty(value = MIN_FREE_SPACE_PERCENT_PROPERTY, defaultValue = DEFAULT_MIN_FREE_SPACE_PERCENT + "")
    private final float minimumSpacePercentageRequired;

    public SSTableUploadConfiguration()
    {
        concurrentUploadsLimit = DEFAULT_CONCURRENT_UPLOAD_LIMIT;
        minimumSpacePercentageRequired = DEFAULT_MIN_FREE_SPACE_PERCENT;
    }

    protected SSTableUploadConfiguration(Builder builder)
    {
        concurrentUploadsLimit = builder.concurrentUploadsLimit;
        minimumSpacePercentageRequired = builder.minimumSpacePercentageRequired;
    }

    /**
     * @return the maximum number of concurrent SSTable component uploads allowed for this service
     */
    @JsonProperty(value = CONCURRENT_UPLOAD_LIMIT_PROPERTY, defaultValue = DEFAULT_CONCURRENT_UPLOAD_LIMIT + "")
    public int concurrentUploadsLimit()
    {
        return concurrentUploadsLimit;
    }

    /**
     * @return the configured minimum space percentage required for an SSTable component upload
     */
    @JsonProperty(value = MIN_FREE_SPACE_PERCENT_PROPERTY, defaultValue = DEFAULT_MIN_FREE_SPACE_PERCENT + "")
    public float minimumSpacePercentageRequired()
    {
        return minimumSpacePercentageRequired;
    }

    @VisibleForTesting
    public Builder unbuild()
    {
        return new Builder(this);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code SSTableUploadConfiguration} builder static inner class.
     */
    public static class Builder implements DataObjectBuilder<Builder, SSTableUploadConfiguration>
    {
        protected int concurrentUploadsLimit = DEFAULT_CONCURRENT_UPLOAD_LIMIT;
        protected float minimumSpacePercentageRequired = DEFAULT_MIN_FREE_SPACE_PERCENT;

        protected Builder()
        {
        }

        protected Builder(SSTableUploadConfiguration configuration)
        {
            concurrentUploadsLimit = configuration.concurrentUploadsLimit;
            minimumSpacePercentageRequired = configuration.minimumSpacePercentageRequired;
        }

        /**
         * Sets the {@code concurrentUploadsLimit} and returns a reference to this Builder enabling method chaining.
         *
         * @param concurrentUploadsLimit the {@code concurrentUploadsLimit} to set
         * @return a reference to this Builder
         */
        public Builder concurrentUploadsLimit(int concurrentUploadsLimit)
        {
            this.concurrentUploadsLimit = concurrentUploadsLimit;
            return this;
        }

        /**
         * Sets the {@code minimumSpacePercentageRequired} and returns a reference to this Builder enabling
         * method chaining.
         *
         * @param minimumSpacePercentageRequired the {@code minimumSpacePercentageRequired} to set
         * @return a reference to this Builder
         */
        public Builder minimumSpacePercentageRequired(float minimumSpacePercentageRequired)
        {
            this.minimumSpacePercentageRequired = minimumSpacePercentageRequired;
            return this;
        }

        /**
         * Returns a {@code SSTableUploadConfiguration} built from the parameters previously set.
         *
         * @return a {@code SSTableUploadConfiguration} built with parameters of this
         * {@code SSTableUploadConfiguration.Builder}
         */
        @Override
        public SSTableUploadConfiguration build()
        {
            return new SSTableUploadConfiguration(this);
        }
    }
}
