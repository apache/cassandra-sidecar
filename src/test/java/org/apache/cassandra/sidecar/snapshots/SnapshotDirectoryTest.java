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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class SnapshotDirectoryTest
{

    @ParameterizedTest
    @ValueSource(strings = { "not-valid", "/two-levels/not-valid", "three/levels/not-valid", "four/levels/not/valid" })
    void failsOnInvalidLengthDirectory()
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> SnapshotDirectory.of("not-valid"))
        .withMessageContaining("Invalid snapshotDirectory. Expected at least 5 parts but found");
    }

    @Test
    void failsOnInvalidDirectory()
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> SnapshotDirectory.of("/cassandra/data/ks1/tbl2/sneaky/test-snapshot"))
        .withMessage("Invalid snapshotDirectory. The expected directory structure is " +
                     "'/<data_dir>/<ks>/<table>/snapshots/<snapshot_name>'");
    }

    @Test
    void testValidDirectory1()
    {
        String snapshotDirectory = "/cassandra/data/ks1/tbl2/snapshots/test-snapshot";
        SnapshotDirectory directory = SnapshotDirectory.of(snapshotDirectory);
        assertThat(directory.dataDirectory).isEqualTo("/cassandra/data");
        assertThat(directory.keyspace).isEqualTo("ks1");
        assertThat(directory.tableName).isEqualTo("tbl2");
        assertThat(directory.snapshotName).isEqualTo("test-snapshot");
    }

    @Test
    void testValidDirectory2()
    {
        String snapshotDirectory = "/cassandra/data/ks1/tbl2/SNAPSHOTS/test-snapshot";
        SnapshotDirectory directory = SnapshotDirectory.of(snapshotDirectory);
        assertThat(directory.dataDirectory).isEqualTo("/cassandra/data");
        assertThat(directory.keyspace).isEqualTo("ks1");
        assertThat(directory.tableName).isEqualTo("tbl2");
        assertThat(directory.snapshotName).isEqualTo("test-snapshot");
    }

    @Test
    void testValidDirectory3()
    {
        String snapshotDirectory = "/datadir/inventory/shipping/snapshots/2022-07-23";
        SnapshotDirectory directory = SnapshotDirectory.of(snapshotDirectory);
        assertThat(directory.dataDirectory).isEqualTo("/datadir");
        assertThat(directory.keyspace).isEqualTo("inventory");
        assertThat(directory.tableName).isEqualTo("shipping");
        assertThat(directory.snapshotName).isEqualTo("2022-07-23");
    }

    @Test
    void testValidDirectory4()
    {
        String snapshotDirectory = "/cassandra/disk1/data/inventory/shipping/snapshots/2022-07-23/";
        SnapshotDirectory directory = SnapshotDirectory.of(snapshotDirectory);
        assertThat(directory.dataDirectory).isEqualTo("/cassandra/disk1/data");
        assertThat(directory.keyspace).isEqualTo("inventory");
        assertThat(directory.tableName).isEqualTo("shipping");
        assertThat(directory.snapshotName).isEqualTo("2022-07-23");
    }
}
