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

package org.apache.cassandra.sidecar.accesscontrol;

import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

/**
 * Manages all {@link AuthCache}.
 */
@Singleton
public class AuthCacheService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthCacheService.class);
    private final boolean enabled;
    private final Set<AuthCache<?, ?>> caches = new HashSet<>();

    @Inject
    public AuthCacheService(SidecarConfiguration sidecarConfiguration)
    {
        this.enabled = sidecarConfiguration.accessControlConfiguration().enabled();
    }

    public synchronized void register(AuthCache<?, ?> cache)
    {
        if (!enabled)
        {
            return;
        }
        Preconditions.checkNotNull(cache, "AuthCache can not be null");
        caches.add(cache);
    }

    public synchronized void warmCaches()
    {
        if (!enabled)
        {
            LOGGER.warn("Access control is disabled in sidecar, hence skipping auth cache warming");
            return;
        }

        LOGGER.info("Initializing bulk load of {} auth caches", caches.size());
        for (AuthCache<?, ?> cache : caches)
        {
            cache.warm();
        }
    }
}
