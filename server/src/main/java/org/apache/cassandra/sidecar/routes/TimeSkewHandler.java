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

package org.apache.cassandra.sidecar.routes;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import com.google.inject.Inject;
import io.vertx.core.Handler;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.SidecarActions;
import org.apache.cassandra.sidecar.acl.authorization.VariableAwareResource;
import org.apache.cassandra.sidecar.utils.TimeSkewInfo;

/**
 * Provides clients information about the current time on this host
 * and the allowable time skew between this host and the client.
 */
public class TimeSkewHandler implements Handler<RoutingContext>, AccessProtected
{
    private final TimeSkewInfo timeSkewInfo;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param timeSkewInfo the time skew info provider
     */
    @Inject
    protected TimeSkewHandler(TimeSkewInfo timeSkewInfo)
    {
        this.timeSkewInfo = timeSkewInfo;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        String resource = VariableAwareResource.CLUSTER.resource();
        return ImmutableSet.of(SidecarActions.VIEW_CLUSTER.toAuthorization(resource));
    }

    @Override
    public void handle(RoutingContext context)
    {
        context.json(timeSkewInfo.timeSkewResponse());
    }
}
