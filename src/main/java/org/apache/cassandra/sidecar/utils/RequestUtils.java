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

package org.apache.cassandra.sidecar.utils;

import io.vertx.core.http.HttpServerRequest;

/**
 * Utility class for Http request related operations.
 */
public class RequestUtils
{
    /**
     * Parses a boolean parameter from the {@code request}, for the given {@code paramName}. If the request param
     * is not {@code true} or {@code false}, it returns the {@code defaultValue}.
     *
     * @param request      the request
     * @param paramName    the name of the query parameter
     * @param defaultValue the default value when the request parameter does not match {@code true} or {@code false}
     * @return the parsed value for the {@code paramName} from the {@code request}
     */
    public static boolean parseBooleanQueryParam(HttpServerRequest request, String paramName, boolean defaultValue)
    {
        String value = request.getParam(paramName);
        if ("true".equalsIgnoreCase(value))
            return true;
        if ("false".equalsIgnoreCase(value))
            return false;
        return defaultValue;
    }

    /**
     * Parses an optional integer parameter from the {@code request}, for the given {@code paramName}.
     *
     * @param request      the request
     * @param paramName    the name of the query parameter
     * @param defaultValue the default value when the parameter does not exist
     * @return the parsed value for the {@code paramName} from the {@code request}
     */
    public static Integer parseIntegerQueryParam(HttpServerRequest request, String paramName, Integer defaultValue)
    {
        String value = request.getParam(paramName);
        if (value != null)
        {
            return Integer.parseInt(value);
        }
        return defaultValue;
    }

    /**
     * Parses an optional integer parameter from the {@code request}, for the given {@code paramName}.
     *
     * @param request      the request
     * @param paramName    the name of the query parameter
     * @param defaultValue the default value when the parameter does not exist
     * @return the parsed value for the {@code paramName} from the {@code request}
     */
    public static Long parseLongQueryParam(HttpServerRequest request, String paramName, Long defaultValue)
    {
        String value = request.getParam(paramName);
        if (value != null)
        {
            return Long.parseLong(value);
        }
        return defaultValue;
    }
}
