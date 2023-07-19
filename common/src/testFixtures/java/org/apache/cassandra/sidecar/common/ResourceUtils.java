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

package org.apache.cassandra.sidecar.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * A utilities class to handle resources for tests
 */
public final class ResourceUtils
{
    /**
     * Writes a resource with {@code resourceName} loaded from the {@link ClassLoader classLoader} into the
     * {@code destinationPath}
     *
     * @param classLoader     the class loader for the resource
     * @param destinationPath the destination path to write the file
     * @param resourceName    the name of the resource to be loaded
     * @return the {@link Path} to the created resource
     */
    public static Path writeResourceToPath(ClassLoader classLoader, Path destinationPath, String resourceName)
    {
        try
        {
            Path resourcePath = destinationPath.resolve(resourceName);

            // ensure parent directory is created
            Files.createDirectories(resourcePath.getParent());

            try (InputStream inputStream = classLoader.getResourceAsStream(resourceName);
                 OutputStream outputStream = Files.newOutputStream(resourcePath))
            {
                assertThat(inputStream).isNotNull();

                int length;
                byte[] buffer = new byte[1024];
                while ((length = inputStream.read(buffer)) != -1)
                {
                    outputStream.write(buffer, 0, length);
                }
            }
            return resourcePath;
        }
        catch (IOException exception)
        {
            String failureMessage = "Unable to create resource " + resourceName;
            fail(failureMessage, exception);
            throw new RuntimeException(failureMessage, exception);
        }
    }
}
