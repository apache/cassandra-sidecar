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

package org.apache.cassandra.sidecar.adapters.base.data;

import java.util.NoSuchElementException;
import javax.management.openmbean.CompositeData;

/**
 * Utility class for operations with {@link CompositeData}
 */
public class CompositeDataUtil
{

    /**
     * Generic helper to extract attribute of a specific type from the CompositeData type.
     * @param data data being parsed
     * @param key attribute being extracted
     * @return attribute value
     * @param <T> return type
     */
    public static <T> T extractValue(CompositeData data, String key)
    {
        Object value = data.get(key);
        if (value == null)
        {
            throw new NoSuchElementException("No value is present for key: " + key);
        }
        try
        {
            return (T) value;
        }
        catch (ClassCastException cce)
        {
            throw new RuntimeException("Value type mismatched of key: " + key, cce);
        }
    }

    /**
     * Safely casts an object to the specified type with descriptive error handling.
     * This method performs a runtime type check before casting to prevent ClassCastException
     * and provides meaningful error messages when the cast fails.
     *
     * @param value the object to be cast
     * @param expectedType the expected type to cast to
     * @param contextDescription descriptive context for error messages (e.g., "keyspace name", "table data")
     * @param <T> the target type
     * @return the cast object of type T
     * @throws IllegalStateException if the value is not an instance of the expected type,
     *         with a descriptive message indicating what was expected vs what was received
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
     * @param value the string value to be parsed
     * @param contextDescription descriptive context for error messages (e.g., "completed bytes", "total bytes")
     * @return the parsed long value
     * @throws IllegalStateException if the value cannot be parsed as a long,
     *         with a descriptive message indicating what failed to parse
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

}
