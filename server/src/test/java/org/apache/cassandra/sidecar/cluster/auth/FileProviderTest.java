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

package org.apache.cassandra.sidecar.cluster.auth;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class FileProviderTest
{
    @TempDir
    Path tempDir;

    @Test
    void testInterfaceGetters() throws IOException
    {
        Path usernamePath = tempDir.resolve("username");
        Path passwordPath = tempDir.resolve("password");
        Files.writeString(usernamePath, "cassandra-user\n");
        Files.writeString(passwordPath, "cassandra-pass\n");

        CqlAuthProvider provider = new FileProvider(Map.of(FileProvider.USERNAME_PATH_PARAM, usernamePath.toString(),
            FileProvider.PASSWORD_PATH_PARAM, passwordPath.toString()));
        assertThat(provider.username()).isEqualTo("cassandra-user");
        assertThat(provider.password()).isEqualTo("cassandra-pass");
    }

    @Test
    void testMissingParameterThrows()
    {
        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(() -> new FileProvider(Map.of(FileProvider.USERNAME_PATH_PARAM, "/tmp/username")))
        .withMessageContaining("Missing required auth_provider parameter");
    }

    @Test
    void testEmptySecretThrows() throws IOException
    {
        Path usernamePath = tempDir.resolve("username");
        Path passwordPath = tempDir.resolve("password");
        Files.writeString(usernamePath, " \n");
        Files.writeString(passwordPath, "secret");

        FileProvider provider = new FileProvider(Map.of(FileProvider.USERNAME_PATH_PARAM, usernamePath.toString(),
                                                                       FileProvider.PASSWORD_PATH_PARAM, passwordPath.toString()));
        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(provider::username)
        .withMessageContaining("Empty content in auth_provider file for parameter");
    }
}
