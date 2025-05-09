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

import java.util.concurrent.CompletableFuture;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.common.response.HealthResponse;
import org.apache.cassandra.sidecar.utils.SidecarClientProvider;


/**
 * Provides the health of a Sidecar instance over HTTP API, retrying to
 * confirm Sidecar is DOWN for extended period of time.
 */
@Singleton
public class SidecarHttpHealthProvider implements SidecarPeerHealthProvider
{
    private final SidecarClientProvider clientProvider;

    @Inject
    public SidecarHttpHealthProvider(SidecarClientProvider clientProvider)
    {
        this.clientProvider = clientProvider;
    }

    @Override
    public Future<Health> health(SidecarInstance instance)
    {
        try
        {
            SidecarClient client = clientProvider.get();
            CompletableFuture<HealthResponse> healthRequest = client.sidecarHealth(instance);
            return Future.fromCompletionStage(healthRequest)
                         .map(healthResponse -> healthResponse.isOk()
                                                ? Health.UP
                                                : Health.DOWN);
        }
        catch (Exception e)
        {
            return Future.succeededFuture(Health.DOWN);
        }
    }
}
