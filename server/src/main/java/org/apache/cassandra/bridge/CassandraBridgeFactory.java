/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.bridge;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.inject.Singleton;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.bridge.BaseCassandraBridgeFactory.getCassandraVersion;

/**
 * Factory class for creating Cassandra bridge instances based on version-specific jar files.
 * <p>
 * This factory maintains a cache of CassandraBridge instances mapped by Cassandra version labels
 * and provides methods to retrieve bridge instances for specific Cassandra versions.
 * Each bridge is loaded from version-specific JAR resources and instantiated using reflection.
 */
@Singleton
public class CassandraBridgeFactory
{
    // maps Cassandra version-specific jar name (e.g. 'four-zero') to matching CassandraBridge
    private final Map<String, CassandraBridge> cassandraBridges;
    
    public CassandraBridgeFactory()
    {
        cassandraBridges = new ConcurrentHashMap<>(CassandraVersion.values().length);
    }

    @NotNull
    public CassandraBridge get(@NotNull String version)
    {
        return get(getCassandraVersion(version));
    }

    @NotNull
    public CassandraBridge get(@NotNull CassandraVersionFeatures features)
    {
        return get(getCassandraVersion(features));
    }

    @NotNull
    public CassandraBridge get(@NotNull CassandraVersion version)
    {
        String jarBaseName = version.jarBaseName();
        Objects.requireNonNull(jarBaseName, "Cassandra version " + version + " is not supported");
        return cassandraBridges.computeIfAbsent(jarBaseName, this::create);
    }

    @NotNull
    @SuppressWarnings("unchecked")
    private CassandraBridge create(@NotNull String label)
    {
        try
        {
            ClassLoader loader = buildClassLoader(
            cassandraResourceName(label),
            bridgeResourceName(label),
            typesResourceName(label));
            Class<CassandraBridge> bridge = (Class<CassandraBridge>) loader.loadClass(CassandraBridge.IMPLEMENTATION_FQCN);
            Constructor<CassandraBridge> constructor = bridge.getConstructor();
            return constructor.newInstance();
        }
        catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
               | IllegalAccessException | InvocationTargetException exception)
        {
            throw new RuntimeException("Failed to create Cassandra bridge for label " + label, exception);
        }
    }

    @NotNull
    String cassandraResourceName(@NotNull String label)
    {
        return "/bridges/" + label + ".jar";
    }

    @NotNull
    String bridgeResourceName(@NotNull String label)
    {
        return jarResourceName(label, "bridge");
    }

    @NotNull
    String typesResourceName(@NotNull String label)
    {
        return jarResourceName(label, "types");
    }

    String jarResourceName(String... parts)
    {
        return "/bridges/" + String.join("-", parts) + ".jar";
    }

    public ClassLoader buildClassLoader(String... resourceNames)
    {
        return AccessController.doPrivileged((PrivilegedAction<ClassLoader>) () ->
                BaseCassandraBridgeFactory.buildClassLoader(resourceNames));
    }

}
