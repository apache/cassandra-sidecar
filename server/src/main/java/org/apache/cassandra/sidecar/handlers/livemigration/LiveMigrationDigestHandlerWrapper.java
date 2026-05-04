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

package org.apache.cassandra.sidecar.handlers.livemigration;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.request.LiveMigrationFileDigestRequest;

import static org.apache.cassandra.sidecar.common.request.LiveMigrationFileDigestRequest.DIGEST_ALGORITHM_PARAM;

/**
 * Wrapper handler that conditionally delegates to {@link LiveMigrationFileDigestHandler} based on the presence
 * of the {@link LiveMigrationFileDigestRequest#DIGEST_ALGORITHM_PARAM} query parameter, otherwise
 * passes control to the next handler in the chain.
 */
@Singleton
public class LiveMigrationDigestHandlerWrapper implements Handler<RoutingContext>
{
    private final LiveMigrationFileDigestHandler liveMigrationFileDigestHandler;

    @Inject
    public LiveMigrationDigestHandlerWrapper(LiveMigrationFileDigestHandler liveMigrationFileDigestHandler)
    {
        this.liveMigrationFileDigestHandler = liveMigrationFileDigestHandler;
    }

    @Override
    public void handle(RoutingContext context)
    {
        if (context.request().params().contains(DIGEST_ALGORITHM_PARAM))
        {
            liveMigrationFileDigestHandler.handle(context);
        }
        else
        {
            context.next();
        }
    }
}
