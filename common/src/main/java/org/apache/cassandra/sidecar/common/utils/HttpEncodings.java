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

import java.util.Objects;

import org.jetbrains.annotations.NotNull;

/**
 * Helper class to group encoding functionality of HTTP-related components
 */
public final class HttpEncodings
{
    /**
     * Encodes the slash character ({@code /}) in the component name. This allows index components to be handled
     * correctly by the matching route. Index components have the form {@code .index_name/nc-1-big-Data.db}.
     *
     * @param componentName the name of the component, including index files
     * @return the encoded componentName
     */
    public static String encodeSSTableComponent(@NotNull String componentName)
    {
        return Objects.requireNonNull(componentName,
                                      "componentName must be provided").replace("/", "%2F");
    }

    /**
     * Decodes the encoded slash character ({@code %2F}) from the component name. This allows for index components
     * to be handled correctly in the stream SStable route. Index components have the form
     * {@code .index_name/nc-1-big-Data.db}.
     *
     * @param encodedComponentName the encoded component name
     * @return the decoded componentName
     */
    public static String decodeSSTableComponent(@NotNull String encodedComponentName)
    {
        return Objects.requireNonNull(encodedComponentName,
                                      "encodedComponentName must be provided").replace("%2F", "/");
    }
}
