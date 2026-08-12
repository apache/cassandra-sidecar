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

package org.apache.cassandra.sidecar.configmanagement;

import org.jetbrains.annotations.NotNull;

/**
 * Thrown when a configuration patch fails due to a hash mismatch,
 * indicating a concurrent modification to the effective configuration.
 */
public class ConfigurationConflictException extends ConfigurationManagerException
{
    @NotNull
    private final String expectedHash;

    @NotNull
    private final String actualHash;

    public ConfigurationConflictException(@NotNull String expectedHash, @NotNull String actualHash)
    {
        super("Configuration conflict: expected hash [" + expectedHash
              + "] but found [" + actualHash + "]", null);
        this.expectedHash = expectedHash;
        this.actualHash = actualHash;
    }

    @NotNull
    public String expectedHash()
    {
        return expectedHash;
    }

    @NotNull
    public String actualHash()
    {
        return actualHash;
    }
}
