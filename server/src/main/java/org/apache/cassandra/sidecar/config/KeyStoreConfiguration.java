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

package org.apache.cassandra.sidecar.config;

import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;

/**
 * Encapsulates key or trust store option configurations
 */
public interface KeyStoreConfiguration
{
    String DEFAULT_TYPE = "JKS";

    /**
     * @return the path to the store
     */
    String path();

    /**
     * @return the password for the store
     */
    String password();

    /**
     * @return the type of the store
     */
    String type();

    /**
     * Returns the interval in which the key store will be checked for filesystem changes. Setting
     * this value to 0 or negative will disable reloading the store.
     *
     * @return the interval in which the key store will be checked for changes in the filesystem
     */
    SecondBoundConfiguration checkInterval();

    /**
     * @return {@code true} if the key store will be reloaded if a change is detected, {@code false} otherwise
     */
    default boolean reloadStore()
    {
        return checkInterval().compareTo(SecondBoundConfiguration.ZERO) > 0;
    }

    /**
     * @return {@code true} if both {@link #path()} and {@link #password()} are provided
     */
    default boolean isConfigured()
    {
        return path() != null && password() != null;
    }
}
