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

package org.apache.cassandra.sidecar.utils;

import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.AssertionUtils;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.exceptions.InsufficientStorageException;
import org.apache.cassandra.sidecar.utils.AsyncFileSystemUtils.FileStoreProps;

import static org.apache.cassandra.sidecar.utils.AsyncFileSystemUtils.ensureSufficientStorage;
import static org.apache.cassandra.sidecar.utils.AsyncFileSystemUtils.fileStoreProps;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AsyncFileSystemUtilsTest
{
    private ExecutorPools executorPools;

    @BeforeEach
    void setup()
    {
        executorPools = new ExecutorPools(Vertx.vertx(), new ServiceConfigurationImpl());
    }

    @AfterEach
    void teardown()
    {
        executorPools.close();
    }

    @Test
    void testReadFileStoreProps()
    {
        FileStoreProps props = AssertionUtils.getBlocking(fileStoreProps(".", executorPools.internal()));
        assertThat(props.name).isNotBlank();

        long total = props.totalSpace;
        long usable = props.usableSpace;
        long unallocated = props.unallocatedSpace;
        assertThat(total)
        .isGreaterThan(usable)
        .isGreaterThan(unallocated)
        .isGreaterThan(0L);

        assertThat(unallocated)
        .isGreaterThanOrEqualTo(usable)
        .isGreaterThan(0L);

        assertThat(usable).isGreaterThan(0L);
    }

    @Test
    void testEnsureSufficientStorage() throws Exception
    {
        // this check should pass (hopefully), as the required usable percentage is 0.0001
        AssertionUtils.getBlocking(ensureSufficientStorage(".", 0L, 0.0001, executorPools.internal()));

        // requesting half of the usable space should pass
        FileStoreProps props = AssertionUtils.getBlocking(fileStoreProps(".", executorPools.internal()));
        AssertionUtils.getBlocking(ensureSufficientStorage(".", props.usableSpace / 2,
                                                           0, executorPools.internal()));

        assertThatThrownBy(() -> AssertionUtils.getBlocking(ensureSufficientStorage(".", Long.MAX_VALUE,
                                                                                    0.0001,
                                                                                    executorPools.internal())))
        .describedAs("Request Long.MAX_VALUE on the local file store should fail")
        .hasRootCauseExactlyInstanceOf(InsufficientStorageException.class)
        .hasMessageContaining("FileStore has insufficient space");

        assertThatThrownBy(() -> AssertionUtils.getBlocking(ensureSufficientStorage(".", 123L,
                                                                                    1.0, executorPools.internal())))
        .describedAs("Require 100% usable disk of the local file store should fail")
        .hasRootCauseExactlyInstanceOf(InsufficientStorageException.class)
        .hasMessageContaining("FileStore has insufficient space");
    }

    @Test
    void testEnsureSufficientStorageWithNonexistingFilePath()
    {
        // The input path `./non-existing + uuid` does not exist.
        // `ensureSufficientStorage` should navigate to parent paths until finding an existing path to be used for checking
        // The test expects no exception is thrown
        AssertionUtils.getBlocking(ensureSufficientStorage("./non-existing" + UUID.randomUUID(), 0L,
                                                           0.0001, executorPools.internal()));
    }
}
