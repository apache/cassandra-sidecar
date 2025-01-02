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

package org.apache.cassandra.sidecar.coordination;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.client.retry.BasicRetryPolicy;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.SidecarPeerHealthConfiguration;
import org.apache.cassandra.sidecar.utils.SidecarClientProvider;


/**
 * Provides the health of a Sidecar instance over HTTP API, retrying to
 * confirm Sidecar is DOWN for extended period of time.
 */
@Singleton
public class SidecarHttpHealthProvider implements SidecarPeerHealthProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarHttpHealthProvider.class);

    private final SidecarClient client;
    private final SidecarPeerHealthConfiguration config;

    @Inject
    public SidecarHttpHealthProvider(SidecarConfiguration sidecarConfiguration, SidecarClientProvider clientProvider)
    {
        config = sidecarConfiguration.sidecarPeerHealthConfiguration();
        client = clientProvider.get();
    }

    protected BasicRetryPolicy retryPolicy()
    {
        return new BasicRetryPolicy(config.healthCheckRetries(), config.healthCheckRetryDelay().toMillis());
    }

    @Override
    public Future<Health> health(SidecarInstance instance)
    {
        try
        {
        BasicRetryPolicy retryPolicy = retryPolicy();
        return Future.fromCompletionStage(client.sidecarInstanceHealth(new SidecarInstanceImpl(instance.hostname(), instance.port()), retryPolicy))
                         .compose(healthResponse -> {
                             if (healthResponse.isOk())
                                 return Future.succeededFuture(Health.OK);
                             return Future.succeededFuture(Health.DOWN);
                         });
        } catch (Exception e)
        {
            return Future.succeededFuture(Health.DOWN);
        }
    }
}
