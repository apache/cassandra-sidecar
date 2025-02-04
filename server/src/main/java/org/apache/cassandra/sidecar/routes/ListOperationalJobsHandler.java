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

import java.util.Collections;
import java.util.Set;
import javax.inject.Inject;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.acl.authorization.VariableAwareResource;
import org.apache.cassandra.sidecar.common.response.ListOperationalJobsResponse;
import org.apache.cassandra.sidecar.common.response.OperationalJobResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.job.OperationalJobManager;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.common.data.OperationalJobStatus.RUNNING;

/**
 * Handler for retrieving the all the jobs running on the sidecar
 */
public class ListOperationalJobsHandler extends AbstractHandler<Void> implements AccessProtected
{
    private final OperationalJobManager jobManager;

    @Inject
    public ListOperationalJobsHandler(InstanceMetadataFetcher metadataFetcher,
                                      ExecutorPools executorPools,
                                      CassandraInputValidator validator,
                                      OperationalJobManager jobManager)
    {
        super(metadataFetcher, executorPools, validator);
        this.jobManager = jobManager;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        String resource = VariableAwareResource.OPERATION.resource();
        return Collections.singleton(BasicPermissions.READ_OPERATIONAL_JOB.toAuthorization(resource));
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }

    @Override
    protected void handleInternal(RoutingContext context, HttpServerRequest httpRequest, @NotNull String host, SocketAddress remoteAddress, Void request)
    {
        ListOperationalJobsResponse listResponse = new ListOperationalJobsResponse();
        jobManager.allInflightJobs()
                  .stream()
                  .map(job -> new OperationalJobResponse(job.jobId(), RUNNING, job.name(), null))
                  .forEach(listResponse::addJob);
        context.json(listResponse);
    }
}
