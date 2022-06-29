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

package org.apache.cassandra.sidecar.snapshots;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.utils.ValidationConfigurationImpl;
import org.apache.cassandra.sidecar.common.utils.ValidationUtils;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class SnapshotPathBuilderTest extends AbstractSnapshotPathBuilderTest
{
    SnapshotPathBuilder initialize(Vertx vertx, InstancesConfig instancesConfig)
    {
        return new SnapshotPathBuilder(vertx, instancesConfig, new ValidationUtils(new ValidationConfigurationImpl()));
    }

    @Test
    void testFindSnapshotDirectories()
    {
        Future<List<String>> future = instance.findSnapshotDirectories("localhost", "a_snapshot");

        VertxTestContext testContext = new VertxTestContext();
        future.onComplete(testContext.succeedingThenComplete());
        // awaitCompletion has the semantics of a java.util.concurrent.CountDownLatch
        try
        {
            assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        assertThat(testContext.failed()).isFalse();
        List<String> snapshotDirectories = future.result();
        Collections.sort(snapshotDirectories);
        assertThat(snapshotDirectories).isNotNull();
        assertThat(snapshotDirectories.size()).isEqualTo(2);
        // we use ends with here, because MacOS prepends the /private path for temporary directories
        assertThat(snapshotDirectories.get(0))
        .endsWith(dataDir0 + "/ks1/a_table-a72c8740a57611ec935db766a70c44a1/snapshots/a_snapshot");
        assertThat(snapshotDirectories.get(1))
        .endsWith(dataDir0 + "/ks1/a_table/snapshots/a_snapshot");
    }
}
