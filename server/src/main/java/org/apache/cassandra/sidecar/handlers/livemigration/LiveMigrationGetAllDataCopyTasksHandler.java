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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Handler;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.response.LiveMigrationDataCopyResponse;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.livemigration.DataCopyTaskManager;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationTask;

import static org.apache.cassandra.sidecar.handlers.AbstractHandler.extractHostAddressWithoutPort;

/**
 * Handler for retrieving all Live Migration data copy tasks.
 */
@Singleton
public class LiveMigrationGetAllDataCopyTasksHandler implements Handler<RoutingContext>, AccessProtected
{
    private final DataCopyTaskManager dataCopyTaskManager;

    @Inject
    public LiveMigrationGetAllDataCopyTasksHandler(DataCopyTaskManager dataCopyTaskManager)
    {
        this.dataCopyTaskManager = dataCopyTaskManager;
    }

    @Override
    public void handle(RoutingContext context)
    {
        String localhost = extractHostAddressWithoutPort(context.request());
        List<LiveMigrationDataCopyResponse> tasks = dataCopyTaskManager.getAllTasks(localhost)
                                                                       .stream()
                                                                       .map(LiveMigrationTask::getResponse)
                                                                       .collect(Collectors.toList());
        context.json(tasks);
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(BasicPermissions.DATA_COPY.toAuthorization());
    }
}
