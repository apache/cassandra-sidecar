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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;

/**
 * Represents a positive time duration. Wrapper class for Cassandra Sidecar duration configuration parameters,
 * providing to the users the opportunity to be able to provide configuration values with a unit of their choice
 * in {@code sidecar.yaml} as per the available options. This class mirrors the Cassandra DurationSpec class,
 * but it differs in that it does not support nanoseconds.
 */
public abstract class DurationSpecImpl implements DurationSpec
{
    /**
     * The Regexp used to parse the duration provided as String.
     */
    private static final Pattern UNITS_PATTERN = Pattern.compile(("^(\\d+)(d|h|s|ms|m)$"));

    private final long quantity;
    private final TimeUnit unit;

    /**
     * Constructs the {@link DurationSpecImpl} with the given {@code value}.
     *
     * @param value the value to parse
     * @throws IllegalArgumentException when the {@code value} can't be parsed, the unit is invalid,
     *                                  or the quantity is invalid
     */
    protected DurationSpecImpl(String value) throws IllegalArgumentException
    {
        Matcher matcher = UNITS_PATTERN.matcher(value);

        if (matcher.find())
        {
            this.quantity = Long.parseLong(matcher.group(1));
            this.unit = DurationSpec.fromSymbol(matcher.group(2));
        }
        else
        {
            throw iae(value);
        }

        validateMinUnit(value, unit, minimumUnit());
        validateQuantity(value, quantity, unit, minimumUnit(), Long.MAX_VALUE);
    }

    /**
     * Constructs the {@link DurationSpecImpl} with the given {@code quantity} and {@code unit}.
     *
     * @param quantity the quantity for the duration
     * @param unit     the unit for the duration
     * @throws IllegalArgumentException when the unit is invalid or the quantity is invalid
     */
    protected DurationSpecImpl(long quantity, TimeUnit unit) throws IllegalArgumentException
    {
        this.quantity = quantity;
        this.unit = unit;

        validateMinUnit(this, unit, minimumUnit());
        validateQuantity(this, quantity, unit, minimumUnit(), Long.MAX_VALUE);
    }

    /**
     * @return the minimum unit that can be represented by this type
     */
    public abstract TimeUnit minimumUnit();

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
    public int hashCode()
    {
        // Milliseconds seems to be a reasonable tradeoff
        return Objects.hash(unit.toMillis(quantity));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof DurationSpec))
            return false;

        DurationSpec that = (DurationSpec) obj;
        if (unit == that.unit())
            return quantity == that.quantity();

        // Due to overflows we can only guarantee that the 2 durations are equal if we get the same results
        // doing the conversion in both directions.
        return unit.convert(that.quantity(), that.unit()) == quantity
               && that.unit().convert(quantity, unit) == that.quantity();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return quantity + DurationSpec.symbol(unit);
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

    void validateMinUnit(Object value, TimeUnit unit, TimeUnit minUnit)
    {
        if (unit.compareTo(minUnit) < 0)
            throw iae(value);
    }

    void validateQuantity(Object value, long quantity, TimeUnit sourceUnit, TimeUnit minUnit, long max)
    {
        if (quantity < 0)
            throw iae(value);

        // no need to validate for negatives as they are not allowed at first place from the regex

        if (minUnit.convert(quantity, sourceUnit) >= max)
            throw new IllegalArgumentException("Invalid duration: " + value + ". It shouldn't be more than " +
                                               (max - 1) + " in " + minUnit.name().toLowerCase());
    }

    IllegalArgumentException iae(Object value)
    {
        return new IllegalArgumentException(String.format("Invalid duration %s. Positive numbers with " +
                                                          "units %s are allowed", value, acceptedUnits(minimumUnit())));
    }

    static String acceptedUnits(TimeUnit minimumUnit)
    {
        TimeUnit[] units = TimeUnit.values();
        return Arrays.stream(Arrays.copyOfRange(units, minimumUnit.ordinal(), units.length))
                     .map(unit -> DurationSpec.symbol(unit) + "(" + unit.name().toLowerCase() + ")")
                     .collect(Collectors.joining(", ", "[", "]"));
    }
}
