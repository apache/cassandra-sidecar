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

import org.junit.jupiter.api.Test;

import static org.apache.cassandra.sidecar.utils.SSTableImporter.DEFAULT_CLEAR_REPAIRED;
import static org.apache.cassandra.sidecar.utils.SSTableImporter.DEFAULT_COPY_DATA;
import static org.apache.cassandra.sidecar.utils.SSTableImporter.DEFAULT_EXTENDED_VERIFY;
import static org.apache.cassandra.sidecar.utils.SSTableImporter.DEFAULT_INVALIDATE_CACHES;
import static org.apache.cassandra.sidecar.utils.SSTableImporter.DEFAULT_RESET_LEVEL;
import static org.apache.cassandra.sidecar.utils.SSTableImporter.DEFAULT_VERIFY_SSTABLES;
import static org.apache.cassandra.sidecar.utils.SSTableImporter.DEFAULT_VERIFY_TOKENS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * Unit tests for the {@link SSTableImporter.ImportOptions.Builder} class
 */
class SSTableImporterOptionsTest
{
    @Test
    void testImportOptionsBuilderFailsWhenRequiredOptionsAreMissing()
    {
        assertThatNullPointerException()
        .isThrownBy(() -> new SSTableImporter.ImportOptions.Builder().build())
        .withMessage("host is required");

        assertThatNullPointerException()
        .isThrownBy(() -> new SSTableImporter.ImportOptions.Builder().host("localhost").build())
        .withMessage("keyspace is required");

        assertThatNullPointerException()
        .isThrownBy(() -> new SSTableImporter.ImportOptions.Builder().host("localhost")
                                                                     .keyspace("ks")
                                                                     .build())
        .withMessage("tableName is required");

        assertThatNullPointerException()
        .isThrownBy(() -> new SSTableImporter.ImportOptions.Builder().host("localhost")
                                                                     .keyspace("ks")
                                                                     .tableName("tbl")
                                                                     .build())
        .withMessage("directory is required");
    }

    @Test
    void testImportOptionsBuilderSucceeds()
    {
        SSTableImporter.ImportOptions options = new SSTableImporter.ImportOptions
                                                    .Builder().host("localhost")
                                                              .keyspace("ks")
                                                              .tableName("tbl")
                                                              .directory("/path/to/sstables")
                                                              .uploadId("0000-0000")
                                                              .build();
        assertThat(options.host).isEqualTo("localhost");
        assertThat(options.keyspace).isEqualTo("ks");
        assertThat(options.tableName).isEqualTo("tbl");
        assertThat(options.directory).isEqualTo("/path/to/sstables");
        assertThat(options.resetLevel).isEqualTo(DEFAULT_RESET_LEVEL);
        assertThat(options.clearRepaired).isEqualTo(DEFAULT_CLEAR_REPAIRED);
        assertThat(options.verifySSTables).isEqualTo(DEFAULT_VERIFY_SSTABLES);
        assertThat(options.verifyTokens).isEqualTo(DEFAULT_VERIFY_TOKENS);
        assertThat(options.invalidateCaches).isEqualTo(DEFAULT_INVALIDATE_CACHES);
        assertThat(options.extendedVerify).isEqualTo(DEFAULT_EXTENDED_VERIFY);
        assertThat(options.copyData).isEqualTo(DEFAULT_COPY_DATA);
    }

    @Test
    void testImportOptionsResetLevel()
    {
        SSTableImporter.ImportOptions options = new SSTableImporter.ImportOptions
                                                    .Builder().host("localhost")
                                                              .keyspace("ks")
                                                              .tableName("tbl")
                                                              .directory("/path/to/sstables")
                                                              .uploadId("0000-0000")
                                                              .resetLevel(false)
                                                              .build();
        assertThat(options.host).isEqualTo("localhost");
        assertThat(options.keyspace).isEqualTo("ks");
        assertThat(options.tableName).isEqualTo("tbl");
        assertThat(options.directory).isEqualTo("/path/to/sstables");
        assertThat(options.resetLevel).isEqualTo(false);
    }

    @Test
    void testImportOptionsClearRepaired()
    {
        SSTableImporter.ImportOptions options = new SSTableImporter.ImportOptions
                                                    .Builder().host("localhost")
                                                              .keyspace("ks")
                                                              .tableName("tbl")
                                                              .directory("/path/to/sstables")
                                                              .uploadId("0000-0000")
                                                              .clearRepaired(false)
                                                              .build();
        assertThat(options.host).isEqualTo("localhost");
        assertThat(options.keyspace).isEqualTo("ks");
        assertThat(options.tableName).isEqualTo("tbl");
        assertThat(options.directory).isEqualTo("/path/to/sstables");
        assertThat(options.clearRepaired).isEqualTo(false);
    }

    @Test
    void testImportOptionsVerifySSTables()
    {
        SSTableImporter.ImportOptions options = new SSTableImporter.ImportOptions
                                                    .Builder().host("localhost")
                                                              .keyspace("ks")
                                                              .tableName("tbl")
                                                              .directory("/path/to/sstables")
                                                              .uploadId("0000-0000")
                                                              .verifySSTables(false)
                                                              .build();
        assertThat(options.host).isEqualTo("localhost");
        assertThat(options.keyspace).isEqualTo("ks");
        assertThat(options.tableName).isEqualTo("tbl");
        assertThat(options.directory).isEqualTo("/path/to/sstables");
        assertThat(options.verifySSTables).isEqualTo(false);
    }

    @Test
    void testImportOptionsVerifyTokens()
    {
        SSTableImporter.ImportOptions options = new SSTableImporter.ImportOptions
                                                    .Builder().host("localhost")
                                                              .keyspace("ks")
                                                              .tableName("tbl")
                                                              .directory("/path/to/sstables")
                                                              .uploadId("0000-0000")
                                                              .verifyTokens(false)
                                                              .build();
        assertThat(options.host).isEqualTo("localhost");
        assertThat(options.keyspace).isEqualTo("ks");
        assertThat(options.tableName).isEqualTo("tbl");
        assertThat(options.directory).isEqualTo("/path/to/sstables");
        assertThat(options.verifyTokens).isEqualTo(false);
    }

    @Test
    void testImportOptionsInvalidateCaches()
    {
        SSTableImporter.ImportOptions options = new SSTableImporter.ImportOptions
                                                    .Builder().host("localhost")
                                                              .keyspace("ks")
                                                              .tableName("tbl")
                                                              .directory("/path/to/sstables")
                                                              .uploadId("0000-0000")
                                                              .invalidateCaches(false)
                                                              .build();
        assertThat(options.host).isEqualTo("localhost");
        assertThat(options.keyspace).isEqualTo("ks");
        assertThat(options.tableName).isEqualTo("tbl");
        assertThat(options.directory).isEqualTo("/path/to/sstables");
        assertThat(options.invalidateCaches).isEqualTo(false);
    }

    @Test
    void testImportOptionsExtendedVerify()
    {
        SSTableImporter.ImportOptions options = new SSTableImporter.ImportOptions
                                                    .Builder().host("localhost")
                                                              .keyspace("ks")
                                                              .tableName("tbl")
                                                              .directory("/path/to/sstables")
                                                              .uploadId("0000-0000")
                                                              .extendedVerify(false)
                                                              .build();
        assertThat(options.host).isEqualTo("localhost");
        assertThat(options.keyspace).isEqualTo("ks");
        assertThat(options.tableName).isEqualTo("tbl");
        assertThat(options.directory).isEqualTo("/path/to/sstables");
        assertThat(options.extendedVerify).isEqualTo(false);
    }

    @Test
    void testImportOptionsCopyData()
    {
        SSTableImporter.ImportOptions options = new SSTableImporter.ImportOptions
                                                    .Builder().host("localhost")
                                                              .keyspace("ks")
                                                              .tableName("tbl")
                                                              .directory("/path/to/sstables")
                                                              .uploadId("0000-0000")
                                                              .copyData(true)
                                                              .build();
        assertThat(options.host).isEqualTo("localhost");
        assertThat(options.keyspace).isEqualTo("ks");
        assertThat(options.tableName).isEqualTo("tbl");
        assertThat(options.directory).isEqualTo("/path/to/sstables");
        assertThat(options.copyData).isEqualTo(true);
    }
}
