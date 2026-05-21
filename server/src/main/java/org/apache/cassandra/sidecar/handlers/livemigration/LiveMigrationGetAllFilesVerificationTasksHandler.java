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
import org.apache.cassandra.sidecar.common.response.LiveMigrationFilesVerificationResponse;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.livemigration.FilesVerificationTaskManager;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationTask;

import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DATA_COPY;
import static org.apache.cassandra.sidecar.handlers.AbstractHandler.extractHostAddressWithoutPort;

/**
 * Handler for retrieving all active live migration files verification tasks.
 * Returns a list of all verification tasks running on the local host.
 */
@Singleton
public class LiveMigrationGetAllFilesVerificationTasksHandler implements Handler<RoutingContext>, AccessProtected
{
    private final FilesVerificationTaskManager taskManager;

    @Inject
    public LiveMigrationGetAllFilesVerificationTasksHandler(FilesVerificationTaskManager taskManager)
    {
        this.taskManager = taskManager;
    }

    @Override
    public void handle(RoutingContext context)
    {
        String localhost = extractHostAddressWithoutPort(context.request());
        List<LiveMigrationFilesVerificationResponse> tasks = taskManager.getAllTasks(localhost)
                                                                        .stream()
                                                                        .map(LiveMigrationTask::getResponse)
                                                                        .collect(Collectors.toList());
        context.json(tasks);
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(DATA_COPY.toAuthorization());
    }
}
