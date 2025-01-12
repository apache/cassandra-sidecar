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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Represents a positive time duration. Wrapper interface for Cassandra Sidecar duration configuration parameters,
 * allowing users the opportunity to configure values with a unit of their choice in {@code sidecar.yaml} as per
 * the available options. This class mirrors the Cassandra DurationSpec class.
 */
public interface DurationSpec extends Comparable<DurationSpec>
{
    /**
     * @param unit the time unit
     * @return the symbol associated with the provide time unit
     * @throws IllegalArgumentException when the {@code unit} is unsupported
     */
    static String symbol(TimeUnit unit)
    {
        switch (unit)
        {
            case DAYS:
                return "d";
            case HOURS:
                return "h";
            case MINUTES:
                return "m";
            case SECONDS:
                return "s";
            case MILLISECONDS:
                return "ms";
            case MICROSECONDS:
                return "us";
            case NANOSECONDS:
                return "ns";
        }
        throw new IllegalArgumentException("Unsupported unit " + unit);
    }

    /**
     * @param symbol the time unit symbol
     * @return the time unit associated to the specified symbol
     * @throws IllegalArgumentException when the {@code symbol} is unsupported
     */
    static TimeUnit fromSymbol(String symbol)
    {
        switch (symbol.toLowerCase())
        {
            case "d":
                return DAYS;
            case "h":
                return HOURS;
            case "m":
                return MINUTES;
            case "s":
                return SECONDS;
            case "ms":
                return MILLISECONDS;
            default:
                throw new IllegalArgumentException(String.format("Unsupported time unit: %s. Supported units are: %s",
                                                                 symbol, Arrays.stream(TimeUnit.values())
                                                                               .map(DurationSpec::symbol)
                                                                               .collect(Collectors.joining(", "))));
        }
    }

    /**
     * @return the amount of time in the specified unit
     */
    long quantity();

    /**
     * @return the time unit to specify the amount of duration
     */
    TimeUnit unit();

    /**
     * @return the minimum unit that can be represented by this type
     */
    TimeUnit minimumUnit();

    /**
     * Converts this duration spec to the {@code targetUnit}.
     * Conversions from finer to coarser granularities lose precision.
     *
     * <p>Conversions from coarser to finer granularities with arguments
     * that would numerically overflow saturate to {@link Long#MIN_VALUE}
     * if negative or {@link Long#MAX_VALUE} if positive.
     *
     * @param targetUnit the target conversion unit
     * @return the converted duration in the {@code targetUnit},
     * or {@code Long.MIN_VALUE} if conversion would negatively overflow,
     * or {@code Long.MAX_VALUE} if it would positively overflow.
     */
    default long to(TimeUnit targetUnit)
    {
        return targetUnit.convert(quantity(), unit());
    }

    /**
     * @return the duration in seconds
     */
    default long toSeconds()
    {
        return to(TimeUnit.SECONDS);
    }
}
