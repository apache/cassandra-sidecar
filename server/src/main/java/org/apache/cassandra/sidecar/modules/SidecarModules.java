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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.google.inject.Module;
import com.google.inject.util.Modules;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
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
     * All sidecar modules for a pre-parsed {@link SidecarConfiguration}.
     * {@link CdcModule} is included unless CDC is explicitly disabled in the configuration.
     * Passing {@code null} (e.g. in tests that supply bindings via {@code Modules.override()})
     * also includes {@link CdcModule}.
     *
     * @param config the already-parsed sidecar configuration, or {@code null} for tests that
     *               configure modules independently
     * @return all applicable sidecar modules
     */
    public static List<Module> all(@Nullable SidecarConfiguration config)
    {
        boolean cdcEnabled = config == null || config.serviceConfiguration().cdcConfiguration().isEnabled();
        return new Builder()
               .add(Modules.disableCircularProxiesModule())
               .add(new ApiModule())
               .add(new AuthModule())
               .add(new CassandraOperationsModule())
               .add(new ConfigurationModule(config))
               .add(new CoordinationModule())
               .add(new HealthCheckModule())
               .add(new LifecycleModule())
               .add(new LiveMigrationModule())
               .add(new MultiBindingTypeResolverModule())
               .add(new OperationalJobsModule())
               .add(new OpenApiModule())
               .add(new RestoreJobModule())
               .add(new SchedulingModule())
               .add(new SchemaReportingModule())
               .add(new SidecarSchemaModule())
               .add(new SSTablesAccessModule())
               .add(new TelemetryModule())
               .add(new UtilitiesModule())
               .add(new SysInfoModule())
               .addIf(cdcEnabled, new CdcModule())
               .build();
    }

    private static class Builder
    {
        private final List<Module> modules = new ArrayList<>();

        Builder add(Module module)
        {
            modules.add(module);
            return this;
        }

        Builder addIf(boolean condition, Module module)
        {
            if (condition)
            {
                modules.add(module);
            }
            return this;
        }

        List<Module> build()
        {
            return List.copyOf(modules);
        }
    }

    /**
     * All sidecar modules, loading configuration from the given YAML path.
     * {@link CdcModule} is included only when CDC is enabled in the parsed configuration.
     *
     * @param confPath path to the configuration file, or {@code null} to omit path-based config
     * @return all applicable sidecar modules
     */
    public static List<Module> all(@Nullable Path confPath)
    {
        if (confPath == null)
        {
            return all((SidecarConfiguration) null);
        }
        try
        {
            return all(SidecarConfigurationImpl.readYamlConfiguration(confPath));
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Failed to read configuration from " + confPath, e);
        }
    }

    /**
     * Similar to {@link #all(Path)}, but not setting the configuration path
     * @return all sidecar modules
     */
    public static List<Module> all()
    {
        return all((SidecarConfiguration) null);
    }
}
