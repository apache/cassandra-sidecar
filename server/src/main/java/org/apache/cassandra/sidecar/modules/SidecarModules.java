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

package org.apache.cassandra.sidecar.modules;

import java.nio.file.Path;
import java.util.List;

import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.cassandra.sidecar.modules.multibindings.MultiBindingTypeResolverModule;
import org.jetbrains.annotations.Nullable;

/**
 * Collection of sidecar modules
 */
public class SidecarModules
{
    private SidecarModules()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * All sidecar modules
     * @param confPath path to the configuration file
     * @return all sidecar modules
     */
    public static List<Module> all(@Nullable Path confPath)
    {
        // To prevent unexpected circular dependency chains in your code, we recommend that you disable Guice's circular proxy feature.
        return List.of(Modules.disableCircularProxiesModule(),
                       new ApiModule(),
                       new AuthModule(),
                       new CassandraOperationsModule(),
                       new CdcModule(),
                       new ConfigurationModule(confPath),
                       new CoordinationModule(),
                       new HealthCheckModule(),
                       new LifecycleModule(),
                       new LiveMigrationModule(),
                       new MultiBindingTypeResolverModule(),
                       new OpenApiModule(),
                       new RestoreJobModule(),
                       new SchedulingModule(),
                       new SchemaReportingModule(),
                       new SidecarSchemaModule(),
                       new SSTablesAccessModule(),
                       new TelemetryModule(),
                       new UtilitiesModule(),
                       new SysInfoModule());
    }

    /**
     * Similar to {@link #all(Path)}, but not setting the configuration path
     * @return all sidecar modules
     */
    public static List<Module> all()
    {
        return all(null);
    }
}
