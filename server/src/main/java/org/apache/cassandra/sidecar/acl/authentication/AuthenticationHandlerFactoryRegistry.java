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

package org.apache.cassandra.sidecar.acl.authentication;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for tracking different {@link AuthenticationHandlerFactory} implementations.
 */
public class AuthenticationHandlerFactoryRegistry
{
    private final Map<String, AuthenticationHandlerFactory> registry = new ConcurrentHashMap<>();

    /**
     * Registers the {@code factory} class in the registry. The registration is made by using
     * the class name.
     *
     * @param factory the factory to register
     */
    public void register(AuthenticationHandlerFactory factory)
    {
        Objects.requireNonNull(factory, "Factory must be non-null");
        registry.put(factory.getClass().getName(), factory);
    }

    /**
     * Returns the registered factory instance for the given {@code className} or {@code null} if none is
     * registered.
     *
     * @param className the name of the factory class name
     * @return the registered factory instance for the given {@code className} or {@code null} if none is
     * registered
     */
    public AuthenticationHandlerFactory getFactory(String className)
    {
        return registry.get(className);
    }
}
