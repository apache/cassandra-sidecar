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

package org.apache.cassandra.sidecar.common.server.utils;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Represents a duration used for Sidecar configuration. The bound is [0, Long.MAX_VALUE) in minutes.
 * If the user sets a different unit - we still validate that converted to minutes the quantity will not exceed
 * that upper bound.
 */
public class MinuteBoundConfiguration extends DurationSpec
{
    @JsonCreator
    public MinuteBoundConfiguration(String value)
    {
        super(value);
    }

    public MinuteBoundConfiguration(long quantity, TimeUnit unit)
    {
        super(quantity, unit);
    }

    /**
     * Parses the {@code value} into a {@link MinuteBoundConfiguration}.
     *
     * @param value the value to parse
     * @return the parsed value into a {@link MinuteBoundConfiguration} object
     */
    public static MinuteBoundConfiguration parse(String value)
    {
        return new MinuteBoundConfiguration(value);
    }

    @Override
    public TimeUnit minimumUnit()
    {
        return TimeUnit.MINUTES;
    }
}
