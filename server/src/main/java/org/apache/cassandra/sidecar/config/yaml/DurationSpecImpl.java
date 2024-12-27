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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.config.DurationSpec;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a positive time duration. Wrapper class for Cassandra Sidecar duration configuration parameters,
 * providing to the users the opportunity to be able to provide configuration values with a unit of their choice
 * in sidecar.yaml as per the available options. This class mirrors the Cassandra DurationSpec class
 */
abstract class DurationSpecImpl implements DurationSpec
{
    /**
     * The Regexp used to parse the duration provided as String.
     */
    private static final Pattern UNITS_PATTERN = Pattern.compile(("^(\\d+)(d|h|s|ms|m)$"));

    private final long quantity;
    private final TimeUnit unit;

    DurationSpecImpl(String value)
    {
        Matcher matcher = UNITS_PATTERN.matcher(value);

        if (matcher.find())
        {
            this.quantity = Long.parseLong(matcher.group(1));
            this.unit = DurationSpec.fromSymbol(matcher.group(2));
        }
        else
        {
            throw new ConfigurationException(String.format("Invalid duration value %s. Only positive numbers with" +
                                                           "the unit of %s are allowed", value, acceptedUnits(minimumUnit())));
        }

        validateMinUnit(unit, minimumUnit());
        validateQuantity(value, quantity, unit, minimumUnit(), Long.MAX_VALUE);
    }

    public DurationSpecImpl(long quantity, TimeUnit unit)
    {
        this.quantity = quantity;
        this.unit = unit;

        validateMinUnit(unit, minimumUnit());
    }

    /**
     * @return the minimum unit that can be represented by this type
     */
    abstract TimeUnit minimumUnit();

    /**
     * {@inheritDoc}
     */
    @Override
    public long quantity()
    {
        return quantity;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TimeUnit unit()
    {
        return unit;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "'" + quantity + "' " + unit.toString().toLowerCase();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(@NotNull DurationSpec that)
    {
        if (this.unit == that.unit())
        {
            return Long.compare(this.quantity, that.quantity());
        }

        TimeUnit minUnit = this.unit.compareTo(that.unit()) < 0 ? this.unit : that.unit();
        return Long.compare(this.to(minUnit), that.to(minUnit));
    }

    void validateMinUnit(TimeUnit unit, TimeUnit minUnit)
    {
        if (unit.compareTo(minUnit) < 0)
            throw new IllegalArgumentException(String.format("Invalid duration: %s Accepted units:%s",
                                                             this, acceptedUnits(minimumUnit())));
    }

    void validateQuantity(String value, long quantity, TimeUnit sourceUnit, TimeUnit minUnit, long max)
    {
        // no need to validate for negatives as they are not allowed at first place from the regex

        if (minUnit.convert(quantity, sourceUnit) >= max)
            throw new IllegalArgumentException("Invalid duration: " + value + ". It shouldn't be more than " +
                                               (max - 1) + " in " + minUnit.name().toLowerCase());
    }

    static String acceptedUnits(TimeUnit minimumUnit)
    {
        TimeUnit[] units = TimeUnit.values();
        return Arrays.stream(Arrays.copyOfRange(units, minimumUnit.ordinal(), units.length))
                     .map(DurationSpec::symbol)
                     .collect(Collectors.joining(", ", "[", "]"));
    }
}
