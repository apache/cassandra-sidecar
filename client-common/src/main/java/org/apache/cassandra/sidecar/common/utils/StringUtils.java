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

package org.apache.cassandra.sidecar.common.utils;

import org.jetbrains.annotations.Nullable;

/**
 * Collection of utility methods for String
 */
public class StringUtils
{
    private StringUtils()
    {
    }

    /**
     * @param string string value to test
     * @return true when the string is null or its length is 0; false otherwise
     */
    public static boolean isNullOrEmpty(@Nullable String string)
    {
        return string == null || string.isEmpty();
    }

    /**
     * @param string string value to test
     * @return true only when the string is not null and its length is not 0; false otherwise
     */
    public static boolean isNotEmpty(@Nullable String string)
    {
        return !isNullOrEmpty(string);
    }
}
