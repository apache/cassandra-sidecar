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

package org.apache.cassandra.sidecar.modules.multibindings;

import io.vertx.core.http.HttpMethod;

/**
 * Used for defining the class key for VertxRoute multi-binding map.
 * <p>The RouteClassKey also defines the contract on the existence of static fields in its implementations.
 * The required static fields are read out using reflection. Below is the list:
 * <ul>
 *     <li>{@code HttpMethod HTTP_METHOD}</li>
 *     <li>{@code String ROUTE_URI}</li>
 * </ul>
 */
public interface RouteClassKey extends ClassKey
{
    String HTTP_METHOD_FIELD_NAME = "HTTP_METHOD";
    String ROUTE_URI_FIELD_NAME = "ROUTE_URI";

    /**
     * Reads the static {@link HttpMethod} defined in the class
     *
     * @param classKey class to read from
     * @return http method
     */
    static HttpMethod httpMethod(Class<? extends RouteClassKey> classKey)
    {
        return readStaticFieldValue(classKey, HTTP_METHOD_FIELD_NAME, HttpMethod.class);
    }

    /**
     * Reads the static route URI string defined in the class
     * @param classKey class to read from
     * @return route URI string
     */
    static String routeURI(Class<? extends RouteClassKey> classKey)
    {
        return readStaticFieldValue(classKey, ROUTE_URI_FIELD_NAME, String.class);
    }

    private static <T> T readStaticFieldValue(Class<? extends RouteClassKey> classKey, String declaredFieldName, Class<T> expectedType)
    {
        try
        {
            return expectedType.cast(classKey.getDeclaredField(declaredFieldName).get(null));
        }
        catch (IllegalAccessException | NoSuchFieldException | ClassCastException e)
        {
            throw new RuntimeException(e);
        }
    }
}
