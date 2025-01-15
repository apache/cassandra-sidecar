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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Represents a duration used for Sidecar configuration. The bound is [0, Long.MAX_VALUE) in milliseconds.
 * If the user sets a different unit - we still validate that converted to milliseconds the quantity will not exceed
 * that upper bound.
 */
public class MillisecondBoundConfiguration extends DurationSpec
{
    /**
     * Represents a 0-millisecond configuration
     */
    public static final MillisecondBoundConfiguration ZERO = new MillisecondBoundConfiguration(0, TimeUnit.MILLISECONDS);

    /**
     * Represents a 1-millisecond configuration
     */
    public static final MillisecondBoundConfiguration ONE = new MillisecondBoundConfiguration(1, TimeUnit.MILLISECONDS);

    public MillisecondBoundConfiguration()
    {
        super(0, MILLISECONDS);
    }

    @JsonCreator
    public MillisecondBoundConfiguration(String value)
    {
        super(value);
    }

    public MillisecondBoundConfiguration(long quantity, TimeUnit unit)
    {
        super(quantity, unit);
    }

    /**
     * Parses the {@code value} into a {@link MillisecondBoundConfiguration}.
     *
     * @param value the value to parse
     * @return the parsed value into a {@link MillisecondBoundConfiguration} object
     */
    public static MillisecondBoundConfiguration parse(String value)
    {
        return new MillisecondBoundConfiguration(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeUnit minimumUnit()
    {
        return MILLISECONDS;
    }
}
