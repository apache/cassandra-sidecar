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

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.MinuteBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.quicktheories.QuickTheory.qt;

/**
 * Unit tests for {@link MillisecondBoundConfiguration} and {@link SecondBoundConfiguration}
 */
class TimeBoundConfigurationImplTest
{
    private static final long MAX_INT_CONFIG_VALUE = Integer.MAX_VALUE - 1;

    @Test
    void testConversions()
    {
        assertThat(MillisecondBoundConfiguration.parse(MAX_INT_CONFIG_VALUE + "ms").toMillis()).isEqualTo(MAX_INT_CONFIG_VALUE);
        assertThat(MillisecondBoundConfiguration.parse(MAX_INT_CONFIG_VALUE + "s").toSeconds()).isEqualTo(MAX_INT_CONFIG_VALUE);
        assertThat(SecondBoundConfiguration.parse(MAX_INT_CONFIG_VALUE + "s").toSeconds()).isEqualTo(MAX_INT_CONFIG_VALUE);
        assertThat(MinuteBoundConfiguration.parse(MAX_INT_CONFIG_VALUE + "m").to(TimeUnit.MINUTES)).isEqualTo(MAX_INT_CONFIG_VALUE);
    }

    @Test
    void testFromSymbol()
    {
        assertThat(DurationSpec.fromSymbol("ms")).isEqualTo(TimeUnit.MILLISECONDS);
        assertThat(DurationSpec.fromSymbol("d")).isEqualTo(TimeUnit.DAYS);
        assertThat(DurationSpec.fromSymbol("h")).isEqualTo(TimeUnit.HOURS);
        assertThat(DurationSpec.fromSymbol("m")).isEqualTo(TimeUnit.MINUTES);
        assertThat(DurationSpec.fromSymbol("s")).isEqualTo(TimeUnit.SECONDS);
    }

    @Test
    void testGetSymbol()
    {
        assertThat(DurationSpec.symbol(TimeUnit.MILLISECONDS)).isEqualTo("ms");
        assertThat(DurationSpec.symbol(TimeUnit.DAYS)).isEqualTo("d");
        assertThat(DurationSpec.symbol(TimeUnit.HOURS)).isEqualTo("h");
        assertThat(DurationSpec.symbol(TimeUnit.MINUTES)).isEqualTo("m");
        assertThat(DurationSpec.symbol(TimeUnit.SECONDS)).isEqualTo("s");
    }

    @ParameterizedTest
    @ValueSource(strings = { "10", "-10s", "10xd", "0.333555555ms" })
    void testInvalidInputs(String value)
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> MillisecondBoundConfiguration.parse(value))
        .withMessageContaining("Invalid duration %s. Positive numbers with units [ms(milliseconds), s(seconds), " +
                               "m(minutes), h(hours), d(days)] are allowed", value);
    }

    @Test
    void testEquals()
    {
        assertThat(MillisecondBoundConfiguration.parse("0ms")).isEqualTo(MillisecondBoundConfiguration.parse("0s"));
        assertThat(MillisecondBoundConfiguration.parse("10s")).isEqualTo(MillisecondBoundConfiguration.parse("10s"));
        assertThat(MillisecondBoundConfiguration.parse("10s")).isEqualTo(new MillisecondBoundConfiguration(10, TimeUnit.SECONDS));
        assertThat(MillisecondBoundConfiguration.parse("1s")).isEqualTo(MillisecondBoundConfiguration.parse("1000ms"));
        assertThat(SecondBoundConfiguration.ONE).isEqualTo(MillisecondBoundConfiguration.parse("1000ms"));
        assertThat(MinuteBoundConfiguration.parse("2m")).isEqualTo(MillisecondBoundConfiguration.parse("120000ms"));
        assertThat(MillisecondBoundConfiguration.parse("10s")).isEqualTo(MillisecondBoundConfiguration.parse("10000ms"));
        assertThat(SecondBoundConfiguration.parse("4h")).isEqualTo(MillisecondBoundConfiguration.parse("14400s"));
        assertThat(SecondBoundConfiguration.parse("0m")).isNotEqualTo(MillisecondBoundConfiguration.parse("10ms"));
        assertThat(MillisecondBoundConfiguration.parse("9223372036854775806ms").toMillis()).isEqualTo(Long.MAX_VALUE - 1);
    }

    @Test
    void thereAndBack()
    {
        Gen<TimeUnit> unitGen = SourceDSL.arbitrary().pick(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES,
                                                           TimeUnit.HOURS, TimeUnit.DAYS);
        long upperbound = TimeUnit.MILLISECONDS.toDays(Long.MAX_VALUE);
        Gen<Long> valueGen = SourceDSL.longs().between(0, upperbound);
        qt().forAll(valueGen, unitGen).check((value, unit) -> {
            DurationSpec there = new MillisecondBoundConfiguration(value, unit);
            DurationSpec back = MillisecondBoundConfiguration.parse(there.toString());
            return there.equals(back);
        });
    }

    @ParameterizedTest(name = "{index} => value={0}")
    @ValueSource(strings = { "10ns", "10us", "10µs", "-10s", "10millis", "10seconds", "10years", "10foo" })
    void testInvalidMillisecondBoundValues(String value)
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> new MillisecondBoundConfiguration(value))
        .withMessageContaining("Invalid duration %s. Positive numbers with units [ms(milliseconds), s(seconds), " +
                               "m(minutes), h(hours), d(days)] are allowed", value);
    }

    @ParameterizedTest(name = "{index} => value={0} unit={1}")
    @MethodSource("invalidUnitsForMillisecondBoundConfiguration")
    void testInvalidMillisecondBoundUnits(long value, TimeUnit unit)
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> new MillisecondBoundConfiguration(value, unit))
        .withMessageContaining("Invalid duration %s%s. Positive numbers with units [ms(milliseconds), s(seconds), " +
                               "m(minutes), h(hours), d(days)] are allowed",
                               value, DurationSpec.symbol(unit));
    }

    @ParameterizedTest(name = "{index} => value={0}")
    @ValueSource(strings = { "10ms", "10ns", "10us", "10µs", "-10s", "10millis", "10seconds", "10years", "10foo" })
    void testInvalidSecondBoundValues(String value)
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> new SecondBoundConfiguration(value))
        .withMessageContaining("Invalid duration %s. Positive numbers with units [s(seconds), m(minutes), h(hours), d(days)] are allowed", value);
    }

    @ParameterizedTest(name = "{index} => value={0}")
    @ValueSource(strings = { "10ms", "10ns", "10us", "10µs", "-10s", "10millis", "10s", "10seconds", "10years", "10foo" })
    void testInvalidMinuteBoundValues(String value)
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> new MinuteBoundConfiguration(value))
        .withMessageContaining("Invalid duration %s. Positive numbers with units [m(minutes), h(hours), d(days)] are allowed", value);
    }

    @ParameterizedTest(name = "{index} => value={0} unit={1}")
    @MethodSource("invalidUnitsForSecondBoundConfiguration")
    void testInvalidSecondBoundUnits(long value, TimeUnit unit)
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> new SecondBoundConfiguration(value, unit))
        .withMessageContaining("Invalid duration %s%s. Positive numbers with units [s(seconds), m(minutes), h(hours), d(days)] are allowed",
                               value, DurationSpec.symbol(unit));
    }

    static Stream<Arguments> invalidUnitsForMillisecondBoundConfiguration()
    {
        return Stream.of(
        Arguments.of(-10, TimeUnit.DAYS),
        Arguments.of(-10, TimeUnit.HOURS),
        Arguments.of(-10, TimeUnit.MINUTES),
        Arguments.of(-10, TimeUnit.SECONDS),
        Arguments.of(-10, TimeUnit.MILLISECONDS)
        );
    }

    static Stream<Arguments> invalidUnitsForSecondBoundConfiguration()
    {
        return Stream.of(
        Arguments.of(-10, TimeUnit.DAYS),
        Arguments.of(-10, TimeUnit.HOURS),
        Arguments.of(-10, TimeUnit.MINUTES),
        Arguments.of(-10, TimeUnit.SECONDS),
        Arguments.of(10, TimeUnit.MILLISECONDS)
        );
    }
}
