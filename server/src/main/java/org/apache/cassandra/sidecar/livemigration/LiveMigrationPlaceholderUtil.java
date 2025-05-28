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

package org.apache.cassandra.sidecar.livemigration;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetbrains.annotations.Nullable;

/**
 * Utility class for replacing placeholder in Live Migration exclusion paths.
 * <p>
 * A placeholder looks like this: "${PLACEHOLDER}/*\/*\/*-Data.db".
 */
public class LiveMigrationPlaceholderUtil
{

    public static final String DATA_FILE_DIR_PLACEHOLDER = "DATA_FILE_DIR";
    public static final String COMMITLOG_DIR_PLACEHOLDER = "COMMITLOG_DIR";
    public static final String HINTS_DIR_PLACEHOLDER = "HINTS_DIR";
    public static final String SAVED_CACHES_DIR_PLACEHOLDER = "SAVED_CACHES_DIR";
    public static final String CDC_RAW_DIR_PLACEHOLDER = "CDC_RAW_DIR";
    public static final String LOCAL_SYSTEM_DATA_FILE_DIR_PLACEHOLDER = "LOCAL_SYSTEM_DATA_FILE_DIR";
    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{(\\w+)}.*");

    /**
     * Replaces a placeholder with a value specified in given input. There can be more than one placeholder
     * pointing to singe value.
     *
     * @param input        input string having placeholder to replace
     * @param placeholders set of placeholders to search for
     * @param value        value with which placeholder will be replaced
     * @return input unchanged if no placeholder is found and input with placeholder replaced if placeholder is in given
     * placeholders, otherwise null.
     */
    @Nullable
    public static String replacePlaceholder(String input, Set<String> placeholders, String value)
    {
        String placeholder = getPlaceholder(input);

        if (placeholder == null)
        {
            return input;
        }
        if (placeholders.contains(placeholder))
        {
            return input.replace("${" + placeholder + "}", value);
        }

        return null;
    }

    /**
     * Checks whether given input has any placeholder init.
     * Placeholders can be specified using {@link #PLACEHOLDER_PATTERN} pattern.
     *
     * @param input input string to test
     * @return true if the given input contains a placeholder, otherwise false
     */
    public static boolean hasAnyPlaceholder(String input)
    {
        String placeholder = getPlaceholder(input);
        return placeholder != null;
    }

    /**
     * Checks whether given input has any of the placeholders specified in {@code knownPlaceHolders}.
     * Placeholders can be specified using {@link #PLACEHOLDER_PATTERN} pattern.
     *
     * @param input             input string to test
     * @param knownPlaceHolders set of placeholders {@code input} is expected to have.
     * @return true if given input contains any one of the knownPlaceHolders, otherwise false
     */
    public static boolean hasAnyPlaceholder(String input, Set<String> knownPlaceHolders)
    {
        String placeholder = getPlaceholder(input);
        return placeholder != null && knownPlaceHolders.contains(placeholder);
    }

    private static @Nullable String getPlaceholder(String input)
    {
        Matcher matcher = PLACEHOLDER_PATTERN.matcher(input);
        if (matcher.find())
        {
            return matcher.group(1);
        }
        return null;
    }
}
