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

/**
 * Utility class for data type conversions.
 */
public class DataTypeUtils
{

    /**
     * Private constructor to prevent instantiation
     */
    private DataTypeUtils()
    {
        // utility class
    }

    /**
     * Converts a metric value to a long representation.
     * This utility method handles common metric value types returned from JMX operations,
     * converting them to a consistent long type for numerical operations.
     *
     * @param value the metric value to convert, must be an Integer or Long
     * @return the value as a long
     * @throws IllegalArgumentException if the value type is not supported (not Integer or Long)
     */
    public static long getValueAsLong(Object value)
    {
        if (value instanceof Integer)
        {
            return ((Integer) value).longValue();
        }
        else if (value instanceof Long)
        {
            return (Long) value;
        }
        else
        {
            throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
        }
    }

    /**
     * Safely casts an object to the specified type with descriptive error handling.
     * This method performs a runtime type check before casting to prevent ClassCastException
     * and provides meaningful error messages when the cast fails.
     *
     * @param value              the object to be cast
     * @param expectedType       the expected type to cast to
     * @param contextDescription descriptive context for error messages (e.g., "keyspace name", "table data")
     * @param <T>                the target type
     * @return the cast object of type T
     * @throws IllegalStateException if the value is not an instance of the expected type,
     *                               with a descriptive message indicating what was expected vs what was received
     */
    public static <T> T safeCast(Object value, Class<T> expectedType, String contextDescription)
    {
        if (!expectedType.isInstance(value))
        {
            throw new ClassCastException("Expected " + expectedType.getSimpleName() + " for " + contextDescription + " but got: " +
                                         (value == null ? "null" : value.getClass().getSimpleName()));
        }
        return expectedType.cast(value);
    }

    /**
     * Safely parses a string to a long with descriptive error handling.
     * This method handles null values and provides meaningful error messages when parsing fails.
     *
     * @param value              the string value to be parsed
     * @param contextDescription descriptive context for error messages (e.g., "completed bytes", "total bytes")
     * @return the parsed long value
     * @throws IllegalStateException if the value cannot be parsed as a long,
     *                               with a descriptive message indicating what failed to parse
     */
    public static long safeParseLong(String value, String contextDescription)
    {
        if (value == null)
        {
            throw new NumberFormatException("Cannot parse null value for " + contextDescription);
        }

        try
        {
            return Long.parseLong(value);
        }
        catch (NumberFormatException ex)
        {
            throw new NumberFormatException("Failed to parse long value '" + value + "' for " + contextDescription + ": " + ex.getMessage());
        }
    }

    /**
     * Converts mebibytes to bytes.
     * This utility method performs the standard conversion from mebibytes to bytes
     * using the binary conversion factor (1 MB = 1024 * 1024 bytes).
     *
     * @param mebibytes the value in mebibytes to convert
     * @return the equivalent value in bytes
     */
    public static long mebibytesToBytes(long mebibytes)
    {
        return mebibytes << 20;
    }
}
