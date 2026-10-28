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

/**
 * Governs behavior when the {@link ConfigurationProvider} is unreachable.
 */
public enum FailurePolicy
{
    /**
     * All operations fall back to the cached configuration.
     * Reads use the cached overlay; writes update the cached overlay.
     *
     * <p><b>Warning:</b> writes performed against the cache while the provider is
     * unavailable are local-only. Once the provider recovers, subsequent reads return
     * the delegate's value, which overwrites the local cache. Any writes made during the
     * outage are therefore lost and are not reconciled back to the delegate.
     */
    CACHED_READ_WRITE,

    /**
     * Reads fall back to the cached overlay; writes are rejected
     * with {@link ConfigurationProviderUnavailableException}.
     */
    CACHED_READ_ONLY,

    /**
     * All operations fail when the provider is unavailable, throwing
     * {@link ConfigurationProviderUnavailableException} so handlers can surface HTTP 503.
     */
    FAIL
}
