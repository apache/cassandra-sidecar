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

/**
 * Configuration class that encapsulates parameters needed for Caches
 */
public interface CacheConfiguration
{
    /**
     * @return the configured amount of time in milliseconds after the entry's creation, the most recent
     * replacement of its value, or its last access has elapsed to be considered an expired entry in the cache
     */
    long expireAfterAccessMillis();

    /**
     * @return the maximum number of entries the cache may contain
     */
    long maximumSize();
}
