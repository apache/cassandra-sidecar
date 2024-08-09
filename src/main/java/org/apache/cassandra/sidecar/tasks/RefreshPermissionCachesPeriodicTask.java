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

package org.apache.cassandra.sidecar.tasks;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Row;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.ext.auth.authorization.AndAuthorization;
import io.vertx.ext.auth.authorization.RoleBasedAuthorization;
import org.apache.cassandra.sidecar.auth.authorization.PermissionsAccessor;
import org.apache.cassandra.sidecar.auth.authorization.SystemAuthDatabaseAccessor;
import org.apache.cassandra.sidecar.config.RoleToSidecarPermissionsConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SERVER_START;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SERVER_STOP;

/**
 * Task to refresh permissions caches periodically
 */
@Singleton
public class RefreshPermissionCachesPeriodicTask implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RefreshPermissionCachesPeriodicTask.class);
    private final EventBus eventBus;
    private final SidecarConfiguration configuration;
    private final PermissionsAccessor permissionsAccessor;
    private final SystemAuthDatabaseAccessor systemAuthDatabaseAccessor;

    @Inject
    public RefreshPermissionCachesPeriodicTask(Vertx vertx,
                                               SidecarConfiguration configuration,
                                               PermissionsAccessor permissionsAccessor,
                                               SystemAuthDatabaseAccessor systemAuthDatabaseAccessor)
    {
        this.eventBus = vertx.eventBus();
        this.permissionsAccessor = permissionsAccessor;
        this.configuration = configuration;
        this.systemAuthDatabaseAccessor = systemAuthDatabaseAccessor;
    }

    /**
     * @return delay in the specified {@link #delayUnit()} for periodic task
     */
    public long delay()
    {
        return configuration.serviceConfiguration().refreshPermissionCachesConfiguration().checkIntervalMillis();
    }

    /**
     * @return the initial delay for the task, defaults to the {@link #delay()}
     */
    public long initialDelay()
    {
        return configuration.serviceConfiguration().refreshPermissionCachesConfiguration().initialDelayMillis();
    }

    /**
     * Defines the task body.
     * The method can be considered as executing in a single thread.
     *
     * <br><b>NOTE:</b> the {@code promise} must be completed (as either succeeded or failed) at the end of the run.
     * Failing to do so, the {@link PeriodicTaskExecutor} will not be able to schedule a new run.
     * See {@link PeriodicTaskExecutor#executeInternal} for details.
     *
     * @param promise a promise when the execution completes
     */
    public void execute(Promise<Void> promise)
    {
        LOGGER.info("Refreshing Permission Caches");

        AsyncLoadingCache<String, Boolean> rolesToSuperUserCache = permissionsAccessor.getRolesToSuperUserCache();
        List<Row> rolesToSuperUserRows = systemAuthDatabaseAccessor.getAllRoles();
        for (Row row : rolesToSuperUserRows)
        {
            CompletableFuture<Boolean> completableFuture = CompletableFuture.completedFuture(row.getBool("is_superuser"));
            rolesToSuperUserCache.put(row.getString("role"), completableFuture);
        }
        rolesToSuperUserCache.put("admin", CompletableFuture.completedFuture(Boolean.TRUE));

        AsyncLoadingCache<String, String> identityToRolesCache = permissionsAccessor.getIdentityToRolesCache();
        List<Row> identityToRolesRows = systemAuthDatabaseAccessor.getAllRolesAndIdentities();
        for (Row row : identityToRolesRows)
        {
            CompletableFuture<String> completableFuture = CompletableFuture.completedFuture(row.getString("role"));
            identityToRolesCache.put(row.getString("identity"), completableFuture);
        }
        Set<String> configIdentities = configuration.authenticatorConfiguration().authorizedIdentities();
        for (String id : configIdentities)
        {
            identityToRolesCache.put(id, CompletableFuture.completedFuture("admin"));
        }

        AsyncLoadingCache<String, AndAuthorization> resourceRoleToPermissionsCache = permissionsAccessor.getRoleToPermissionsCache();
        List<Row> resourceRoleToPermissionsRows = systemAuthDatabaseAccessor.getAllPermissionsFromResourceRole();
        for (Row row : resourceRoleToPermissionsRows)
        {
            CompletableFuture<AndAuthorization> permissionsCompletableFuture = resourceRoleToPermissionsCache.getIfPresent(row.getString("role"));
            AndAuthorization permissions;
            if (permissionsCompletableFuture == null)
            {
                permissions = AndAuthorization.create();
            }
            else
            {
                try
                {
                    permissions = permissionsCompletableFuture.get();
                }
                catch (InterruptedException | ExecutionException e)
                {
                    permissions = AndAuthorization.create();
                }
            }
            for (String permission : row.getSet("permissions", String.class))
            {
                String[] splitResource = row.getString("resource").split("/");
                String resource = "<";
                if (splitResource[0].equals("data"))
                {
                    if (splitResource.length == 2)
                    {
                        resource += "keyspace " + splitResource[1] + ">";
                    }
                    else if (splitResource.length == 3)
                    {
                        resource += "table " + splitResource[1] + "." + splitResource[2] + ">";
                    }
                }
                else if (splitResource[0].equals("mbean"))
                {
                    resource += "mbean " + splitResource[1] + ">";
                }
                else if (splitResource[0].equals("role"))
                {
                    resource += "role " + splitResource[1] + ">";
                }

                RoleBasedAuthorization auth = RoleBasedAuthorization.create(permission);
                if (!resource.equals("<"))
                {
                    auth.setResource(resource);
                }

                if (auth.getResource() != null && !permissions.verify(auth))
                {
                    permissions.addAuthorization(auth);
                }
            }
            resourceRoleToPermissionsCache.put(row.getString("role"), CompletableFuture.completedFuture(permissions));
        }
        AndAuthorization allPermissions = AndAuthorization.create()
                                                          .addAuthorization(RoleBasedAuthorization.create("ALL PERMISSIONS")
                                                                                                  .setResource("ALL FUNCTIONS"));
        resourceRoleToPermissionsCache.put("admin", CompletableFuture.completedFuture(allPermissions));

        for (RoleToSidecarPermissionsConfiguration sidecarAuth : configuration.authorizerConfiguration().roleToSidecarPermissions())
        {
            resourceRoleToPermissionsCache.put(sidecarAuth.role(), CompletableFuture.completedFuture(sidecarAuth.permissions()));
        }

        permissionsAccessor.setCaches(rolesToSuperUserCache,
                                      identityToRolesCache,
                                      resourceRoleToPermissionsCache);
        promise.complete();
    }

    /**
     * Register the periodic task executor at the task. By default, it is no-op.
     * If the reference to the executor is needed, the concrete {@link PeriodicTask} can implement this method
     *
     * @param executor the executor that manages the task
     */
    public void registerPeriodicTaskExecutor(PeriodicTaskExecutor executor)
    {
        eventBus.localConsumer(ON_SERVER_START.address(), message -> executor.schedule(this));
        eventBus.localConsumer(ON_SERVER_STOP.address(), message -> executor.unschedule(this));
    }

    /**
     * @return descriptive name of the task. It prefers simple class name, if it is non-empty;
     * otherwise, it returns the full class name
     */
    public String name()
    {
        return "Refresh Permission Caches";
    }
}
