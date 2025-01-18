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

package org.apache.cassandra.sidecar.modules.multibindings;

import java.util.Map;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.db.schema.TableSchema;
import org.apache.cassandra.sidecar.routes.VertxRoute;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;

/**
 * Provides the capability to resolve bindings in the map binder
 * <p>This module employs the {@link IdentityMultiBindingTypeResolver}, which does not modify the bindings in the map binder.
 * <p>If it is desired to update the bindings in the map binder, a custom {@link MultiBindingTypeResolver} can be provided to
 * override the provided resolver from this module.
 */
public class MultiBindingTypeResolverModule extends AbstractModule
{
    @Provides
    @Singleton
    MultiBindingTypeResolver<VertxRoute> routeTypeResolver(Map<Class<? extends ClassKey>, VertxRoute> vertxRouteMap)
    {
        return new IdentityMultiBindingTypeResolver<>(vertxRouteMap);
    }

    @Provides
    @Singleton
    MultiBindingTypeResolver<TableSchema> sidecarTableSchemaTypeResolver(Map<Class<? extends ClassKey>, TableSchema> tableSchemaMap)
    {
        return new IdentityMultiBindingTypeResolver<>(tableSchemaMap);
    }

    @Provides
    @Singleton
    MultiBindingTypeResolver<PeriodicTask> periodicTaskTypeResolver(Map<Class<? extends ClassKey>, PeriodicTask> periodicTaskMap)
    {
        return new IdentityMultiBindingTypeResolver<>(periodicTaskMap);
    }
}
