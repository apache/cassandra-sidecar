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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for the {@link CassandraSidecarDaemon}
 */
class CassandraSidecarDaemonTest
{
    @BeforeEach
    void setup()
    {
        System.clearProperty("sidecar.config");
    }

    @Test
    void testStartFailsWithInvalidURI()
    {
        System.setProperty("sidecar.config", "file://./invalid/URI");

        assertThatIllegalArgumentException().isThrownBy(() -> CassandraSidecarDaemon.main(null))
                                            .withMessage("Invalid URI: file://./invalid/URI");
    }

    @Test
    void testStartFailsWithNonExistentFile()
    {
        System.setProperty("sidecar.config", "file:///tmp/file/does/not/exist.yaml");

        assertThatIllegalArgumentException().isThrownBy(() -> CassandraSidecarDaemon.main(null))
                                            .withMessage("Sidecar configuration file '/tmp/file/does/not/exist.yaml' does not exist");
    }
}
