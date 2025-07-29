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

package org.apache.cassandra.testing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.shared.Versions;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Wraps functionality for the DTestJarClassLoader
 */
public class IsolatedDTestClassLoaderWrapper
{
    protected DTestJarClassLoader dtestJarClassLoader;

    /**
     * Initialize dtest jar class loader
     *
     * @param testVersion version to dtest jar
     * @param clazz       has to be a class in the cassandra-analytics-integration-framework package
     */
    public void initializeDTestJarClassLoader(TestVersion testVersion, Class<?> clazz)
    {
        ClassLoader parent = Thread.currentThread().getContextClassLoader();
        Semver version = new Semver(testVersion.version(), Semver.SemverType.LOOSE);
        Versions versions = Versions.find();
        assertThat(versions).as("No dtest jar versions found").isNotNull();
        List<URL> urlList = new ArrayList<>(Arrays.asList(versions.getLatest(version).classpath));
        URL classUrl = urlOfClass(clazz);
        urlList.add(classUrl);
        dtestJarClassLoader =
        AccessController.doPrivileged((PrivilegedAction<DTestJarClassLoader>) () ->
                                                                              new DTestJarClassLoader(urlList.toArray(new URL[0]), parent));
    }

    public void closeDTestJarClassLoader()
    {
        if (dtestJarClassLoader != null)
        {
            try
            {
                dtestJarClassLoader.close();
            }
            catch (IOException ioException)
            {
                throw new UncheckedIOException("Failed to close custom class loader", ioException);
            }
        }
    }

    public <T> T executeActionOnDTestClassLoader(ExecutableAction<T> action)
    {
        Thread currentThread = Thread.currentThread();
        ClassLoader originalClassLoader = currentThread.getContextClassLoader();
        try
        {
            currentThread.setContextClassLoader(dtestJarClassLoader);
            return action.run();
        }
        finally
        {
            currentThread.setContextClassLoader(originalClassLoader);
        }
    }

    @SuppressWarnings("unchecked")
    public IClusterExtension<? extends IInstance> loadCluster(String versionString,
                                                              ClusterBuilderConfiguration builderConfiguration)
    {
        return executeActionOnDTestClassLoader(() -> {
            try
            {
                Class<?> launcherClass = Class.forName("org.apache.cassandra.distributed.impl.CassandraCluster", true, dtestJarClassLoader);
                Constructor<IClusterExtension<? extends IInstance>> ctor =
                (Constructor<IClusterExtension<? extends IInstance>>) launcherClass.getDeclaredConstructor(String.class,
                                                                                                           ClusterBuilderConfiguration.class);
                return ctor.newInstance(versionString, builderConfiguration);
            }
            catch (ReflectiveOperationException e)
            {
                throw new RuntimeException("Unable to provision cluster for version " + versionString, e);
            }
        });
    }

    /**
     * Inner class loader to isolate the dtest jar classes
     */
    @VisibleForTesting
    public static class DTestJarClassLoader extends URLClassLoader
    {
        DTestJarClassLoader(URL[] urls, ClassLoader parent)
        {
            super(urls, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException
        {
            if (!"org.apache.cassandra.distributed.impl.CassandraCluster".equals(name))
            {
                return super.loadClass(name, resolve);
            }

            // First, check if the class has already been loaded
            Class<?> type = findLoadedClass(name);
            if (type == null)
            {
                try
                {
                    type = findClass(name);
                }
                catch (ClassNotFoundException | SecurityException | LinkageError exception)
                {
                    // ClassNotFoundException thrown if class not found
                }
                if (type == null)
                {
                    // If not found, then invoke findClass in order to find the class
                    type = super.loadClass(name, false);
                }
            }
            if (resolve)
            {
                resolveClass(type);
            }
            return type;
        }
    }

    static URL urlOfClass(Class<?> clazz)
    {
        URL classUrl = clazz.getResource(clazz.getSimpleName() + ".class");
        assertThat(classUrl).isNotNull();
        String pathOfClassInJar = "!/" + clazz.getCanonicalName().replace(".", "/") + ".class";
        String classDir = classUrl.getPath().replace(pathOfClassInJar, "");
        try
        {
            return new URL(classDir);
        }
        catch (MalformedURLException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Encapsulates action to be executed inside the classloader
     *
     * @param <T> return type
     */
    public interface ExecutableAction<T>
    {
        /**
         * @return (optional) result from the operation that runs inside the classloader
         */
        T run();
    }

    /**
     * Encapsulates an action that can throw an {@link IOException} while executing inside the classloader
     *
     * @param <T> return type
     */
    public interface ExecutableExceptionableAction<T>
    {
        /**
         * @return (optional) result from the operation that runs inside the classloader
         * @throws IOException when an IO exception occurs while running this operation
         */
        T run() throws IOException;
    }
}
