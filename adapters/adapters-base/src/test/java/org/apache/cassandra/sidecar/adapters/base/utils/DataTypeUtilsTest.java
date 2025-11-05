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

package org.apache.cassandra.sidecar.adapters.base.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link DataTypeUtils}
 */
public class DataTypeUtilsTest
{
    private static final String TEST_CONTEXT = "test context";

    static Stream<Arguments> safeCastTestCases()
    {
        Map<String, Object> testMap = new HashMap<>();
        testMap.put("key", "value");

        return Stream.of(
        // Test casting a string to String class - should succeed
        Arguments.of("successful string cast", "test string", String.class, "test string", null),
        // Test casting a Map to Map class - should succeed
        Arguments.of("successful map cast", testMap, Map.class, testMap, null),
        // Test casting an Integer to Integer class - should succeed
        Arguments.of("successful integer cast", 42, Integer.class, 42, null),
        // Test casting a String to Integer class - should throw ClassCastException
        Arguments.of("class cast exception - wrong type", "not a number", Integer.class, null,
                     String.format("Expected Integer for %s but got: String", TEST_CONTEXT)),
        // Test casting null to String class - should throw ClassCastException
        Arguments.of("class cast exception - null input", null, String.class, null,
                     String.format("Expected String for %s but got: null", TEST_CONTEXT))
        );
    }

    @ParameterizedTest
    @MethodSource("safeCastTestCases")
    void testSafeCast(String testCase, Object input, Class<?> targetClass, Object expectedResult, String expectedExceptionMessage)
    {
        if (expectedExceptionMessage == null)
        {
            Object result = DataTypeUtils.safeCast(input, targetClass, TEST_CONTEXT);
            assertEquals(expectedResult, result);
        }
        else
        {
            ClassCastException exception = assertThrows(ClassCastException.class, () ->
                                                                                  DataTypeUtils.safeCast(input, targetClass, TEST_CONTEXT));
            assertEquals(expectedExceptionMessage, exception.getMessage());
        }
    }

    static Stream<Arguments> safeParseLongTestCases()
    {
        // Testcase, input String to parse, expected long output, expected exception message
        return Stream.of(
        // Test parsing a valid positive number
        Arguments.of("successful parse - positive number", "12345", 12345L, null),
        // Test parsing a valid negative number
        Arguments.of("successful parse - negative number", "-9876", -9876L, null),
        // Test parsing zero
        Arguments.of("successful parse - zero", "0", 0L, null),
        // Test parsing the maximum long value
        Arguments.of("successful parse - max long value", String.valueOf(Long.MAX_VALUE), Long.MAX_VALUE, null),
        // Test parsing the minimum long value
        Arguments.of("successful parse - min long value", String.valueOf(Long.MIN_VALUE), Long.MIN_VALUE, null),
        // Test parsing null input - should throw NumberFormatException
        Arguments.of("exception - null input", null, null,
                     String.format("Cannot parse null value for %s", TEST_CONTEXT)),
        // Test parsing non-numeric string - should throw NumberFormatException
        Arguments.of("exception - invalid format", "not a number", null,
                     String.format("Failed to parse long value 'not a number' for %s: For input string: \"not a number\"", TEST_CONTEXT)),
        // Test parsing empty string - should throw NumberFormatException
        Arguments.of("exception - empty string", "", null,
                     String.format("Failed to parse long value '' for %s: For input string: \"\"", TEST_CONTEXT)),
        // Test parsing string with leading/trailing spaces - should throw NumberFormatException
        Arguments.of("exception - string with spaces", " 123 ", null,
                     String.format("Failed to parse long value ' 123 ' for %s: For input string: \" 123 \"", TEST_CONTEXT)),
        // Test parsing number larger than Long.MAX_VALUE - should throw NumberFormatException
        Arguments.of("exception - out of range", "9223372036854775808", null,
                     String.format("Failed to parse long value '9223372036854775808' for %s: For input string: \"9223372036854775808\"", TEST_CONTEXT))
        );
    }

    @ParameterizedTest
    @MethodSource("safeParseLongTestCases")
    void testSafeParseLong(String testCase, String input, Long expectedResult, String expectedExceptionMessage)
    {
        if (expectedExceptionMessage == null)
        {
            long result = DataTypeUtils.safeParseLong(input, TEST_CONTEXT);
            assertEquals(expectedResult, result);
        }
        else
        {
            NumberFormatException exception = assertThrows(NumberFormatException.class, () ->
                                                                                        DataTypeUtils.safeParseLong(input, TEST_CONTEXT));
            assertEquals(expectedExceptionMessage, exception.getMessage());
        }
    }

    static Stream<Arguments> mebibytesToBytesTestCases()
    {
        return Stream.of(
        // Test converting 0 megabytes
        Arguments.of("zero megabytes", 0L, 0L),
        // Test converting 1 megabyte
        Arguments.of("one megabyte", 1L, 1024L * 1024L),
        // Test converting 5 megabytes
        Arguments.of("five megabytes", 5L, 5L * 1024L * 1024L),
        // Test converting large value
        Arguments.of("large value", 100L, 100L * 1024L * 1024L)
        );
    }

    @ParameterizedTest
    @MethodSource("mebibytesToBytesTestCases")
    void testMebibytesToBytes(String testCase, long megabytes, long expectedBytes)
    {
        long result = DataTypeUtils.mebibytesToBytes(megabytes);
        assertEquals(expectedBytes, result);
    }

    static Stream<Arguments> isValidBigIntTestCases()
    {
        return Stream.of(
        // Test null input - should return false
        Arguments.of("null input", null, false),
        // Test valid positive integer
        Arguments.of("valid positive integer", "123", true),
        // Test valid negative integer
        Arguments.of("valid negative integer", "-456", true),
        // Test zero
        Arguments.of("valid zero", "0", true),
        // Test very large positive number (beyond Long.MAX_VALUE)
        Arguments.of("valid large positive number", "12345678901234567890123456789", true),
        // Test very large negative number (beyond Long.MIN_VALUE)
        Arguments.of("valid large negative number", "-98765432109876543210987654321", true),
        // Test number with leading whitespace - should be valid after trim
        Arguments.of("valid with leading whitespace", "  789", true),
        // Test number with trailing whitespace - should be valid after trim
        Arguments.of("valid with trailing whitespace", "321  ", true),
        // Test number with leading and trailing whitespace - should be valid after trim
        Arguments.of("valid with leading and trailing whitespace", "  999  ", true),
        // Test number with just whitespace and sign
        Arguments.of("valid negative with whitespace", "  -555  ", true),
        // Test empty string - should be invalid
        Arguments.of("invalid empty string", "", false),
        // Test string with only whitespace - should be invalid
        Arguments.of("invalid whitespace only", "   ", false),
        // Test string with alphabetic characters
        Arguments.of("invalid alphabetic", "abc", false),
        // Test string with mixed alphanumeric
        Arguments.of("invalid alphanumeric", "123abc", false),
        // Test string with special characters
        Arguments.of("invalid special characters", "123!@#", false),
        // Test string with decimal point
        Arguments.of("invalid decimal", "123.45", false),
        // Test string with scientific notation
        Arguments.of("invalid scientific notation", "1e5", false),
        // Test string with multiple signs
        Arguments.of("invalid multiple signs", "--123", false),
        // Test string with sign in middle
        Arguments.of("invalid sign in middle", "12-3", false),
        // Test string with plus sign (valid)
        Arguments.of("valid with plus sign", "+123", true),
        // Test string with hex prefix
        Arguments.of("invalid hex prefix", "0x123", false)
        );
    }

    @ParameterizedTest
    @MethodSource("isValidBigIntTestCases")
    void testIsValidBigInt(String testCase, String input, boolean expectedResult)
    {
        boolean result = DataTypeUtils.isValidBigInt(input);
        assertEquals(expectedResult, result);
    }
}
