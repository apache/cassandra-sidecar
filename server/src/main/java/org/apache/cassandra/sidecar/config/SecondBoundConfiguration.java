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

import org.apache.cassandra.sidecar.config.yaml.SecondBoundConfigurationImpl;

/**
 * Represents a duration used for Sidecar configuration. The bound is [0, Long.MAX_VALUE) in seconds.
 * If the user sets a different unit - we still validate that converted to seconds the quantity will not exceed
 * that upper bound.
 */
public interface SecondBoundConfiguration extends DurationSpec
{
    /**
     * Represents a 0-seconds configuration
     */
    SecondBoundConfiguration ZERO = SecondBoundConfiguration.parse("0s");

    /**
     * Represents a 1-second configuration
     */
    SecondBoundConfiguration ONE = SecondBoundConfiguration.parse("1s");

    static SecondBoundConfiguration parse(String value)
    {
        return new SecondBoundConfigurationImpl(value);
    }

    /**
     * @return the duration in seconds
     */
    default long toSeconds()
    {
        return unit().toSeconds(quantity());
    }
}
