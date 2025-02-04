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

package org.apache.cassandra.sidecar;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.cassandra.sidecar.server.Server;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for the {@link CassandraSidecarDaemon}
 */
class CassandraSidecarDaemonTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraSidecarDaemonTest.class);

    static final String[] NO_ARGS = {};

    @BeforeEach
    void setup()
    {
        System.clearProperty("sidecar.config");
    }

    @Test
    void testStartFailsWithInvalidURI()
    {
        System.setProperty("sidecar.config", "file://./invalid/URI");

        assertThatIllegalArgumentException().isThrownBy(() -> CassandraSidecarDaemon.main(NO_ARGS))
                                            .withMessage("Invalid URI: file://./invalid/URI");
    }

    @Test
    void testStartFailsWithNonExistentFile()
    {
        System.setProperty("sidecar.config", "file:///tmp/file/does/not/exist.yaml");

        assertThatIllegalArgumentException().isThrownBy(() -> CassandraSidecarDaemon.main(NO_ARGS))
                                            .withMessage("Sidecar configuration file '/tmp/file/does/not/exist.yaml' does not exist");
    }

    @Test
    void testSuccessfulStartup()
    {
        Path path = Paths.get("../conf/sidecar.yaml");
        assertThat(path).exists();

        System.setProperty("sidecar.config", path.toUri().toString());
        try
        {
            CassandraSidecarDaemon.main(NO_ARGS);

            WebClient client = WebClient.create(Vertx.vertx());
            loopAssert(10, () -> {
                HttpResponse<String> response = getBlocking(client.get(9043, "localhost", "/api/v1/__health")
                                                                  .as(BodyCodec.string())
                                                                  .send(),
                                                            2, TimeUnit.SECONDS,
                                                            "Query for sidecar health");
                assertThat(response.statusCode()).isEqualTo(OK.code());
                assertThat(response.body()).isEqualTo("{\"status\":\"OK\"}");
            });
        }
        finally
        {
            maybeStopCassandraSidecar();
        }
    }

    @Test
    void testSuccessfulStartupWithDefaultPath() throws Exception
    {
        Path path = Paths.get("../conf/sidecar.yaml");
        assertThat(path).exists();

        // First ensure startup fails because the conf file does not exist
        assertThatIllegalArgumentException().isThrownBy(() -> CassandraSidecarDaemon.main(NO_ARGS))
                                            .withMessageMatching("Sidecar configuration file '.*/conf/sidecar.yaml' does not exist");

        // Now let's copy the file to the expected location
        Path targetFile = Paths.get("conf/sidecar.yaml");
        List<Path> createdParents = null;

        try
        {
            createdParents = createParents(targetFile.toAbsolutePath());
            Files.copy(path.toAbsolutePath(), targetFile.toAbsolutePath());

            CassandraSidecarDaemon.main(NO_ARGS);

            WebClient client = WebClient.create(Vertx.vertx());
            loopAssert(10, () -> {
                HttpResponse<String> response = getBlocking(client.get(9043, "localhost", "/api/v1/__health")
                                                                  .as(BodyCodec.string())
                                                                  .send(),
                                                            2, TimeUnit.SECONDS,
                                                            "Query for sidecar health");
                assertThat(response.statusCode()).isEqualTo(OK.code());
                assertThat(response.body()).isEqualTo("{\"status\":\"OK\"}");
            });
        }
        finally
        {
            LOGGER.debug("Tearing down");
            maybeStopCassandraSidecar();
            Files.deleteIfExists(targetFile);

            if (createdParents != null)
            {
                for (Path createdParent : createdParents)
                {
                    Files.deleteIfExists(createdParent);
                }
            }
        }
    }

    static void maybeStopCassandraSidecar()
    {
        Server runningApplication = CassandraSidecarDaemon.runningApplication;
        if (runningApplication != null)
        {
            CassandraSidecarDaemon.close(runningApplication);
        }
    }

    static List<Path> createParents(Path file) throws IOException
    {
        List<Path> createdParents = new ArrayList<>();
        Path parentDirectory = file.getParent();
        if (parentDirectory == null)
        {
            return createdParents;
        }
        Path directory = parentDirectory;

        while (directory != null && !Files.exists(directory))
        {
            createdParents.add(directory);
            directory = directory.getParent();
        }
        Files.createDirectories(parentDirectory);
        return createdParents;
    }
}
