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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.cassandra.sidecar.exceptions.CassandraInputException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test validation methods.
 */
abstract class CassandraInputValidatorTest
{
    CassandraInputValidator instance;

    @BeforeEach
    void setup()
    {
        instance = initializeInstanceForTest();
    }

    protected abstract CassandraInputValidator initializeInstanceForTest();

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "test_table_name", "\"test_table_name\"", "testTableName", "\"testTableName\"", "a_",
                             "\"cycling\"", "\"Helmets\"", "\"mIxEd_cAsE\"", "a8", "a", "\"8a\"",
                             "\"_must_begin_with_alphabetic_unless_quoted_p\"" })
    void testValidTableNameValidation(String tableName)
    {
        instance.validateTableName(tableName);
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "test table", "_must_begin_with_alphabetic", "dash-is-not-allowed", "\"",
                             "\"inv@lid_chars\"", "test:table_name", "test-table$name", "8a", "testTable/Name" })
    void failsWithInvalidTableName(String tableName)
    {
        assertThatExceptionOfType(CassandraInputException.class)
        .isThrownBy(() -> instance.validateTableName(tableName))
        .withMessage("Invalid characters in table name: " + tableName);
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "SystemViews", "system_views_test", "\"keyspace\"", "\"cycling\"", "\"Cycling\"",
                             "\"mIxEd_cAsE\"", "a8", "a", "a_", "\"_a\"", "keyspace_name_can_only_have_48_characters_345678" })
    void testValidKeyspaceValidation(String keyspace)
    {
        instance.validateKeyspaceName(keyspace);
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "system_schema",
                             "system_traces",
                             "system_distributed",
                             "system",
                             "system_auth",
                             "system_views",
                             "system_virtual_schema" })
    void failsWithForbiddenKeyspace(String keyspace)
    {
        assertThatExceptionOfType(CassandraInputException.class).isThrownBy(() -> instance.validateKeyspaceName(keyspace))
                                                                .withMessage("Forbidden keyspace: " + keyspace);
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "test keyspace", "_cycling", "dash-is-not-allowed", "\"", "\"inv@lid_chars\"", "8a" })
    void failsWithInvalidKeyspaceName(String keyspace)
    {
        assertThatExceptionOfType(CassandraInputException.class)
        .isThrownBy(() -> instance.validateKeyspaceName(keyspace))
        .withMessage("Invalid characters in keyspace: " + keyspace);
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "test-file-name.db", "test_file_name.json", "testFileName.cql", "t_TOC.txt", "crcfile.crc32" })
    void testValidateFileName_validFileNames_expectNoException(String componentName)
    {
        instance.validateComponentName(componentName);
    }

    private void testCommon_testInvalidFileName(String testFileName)
    {
        assertThatExceptionOfType(CassandraInputException.class).isThrownBy(() -> instance.validateComponentName(testFileName))
                                                                .withMessage("Invalid component name: " + testFileName);
    }

    @Test
    void testValidateFileName_withoutExtension_expectException()
    {
        testCommon_testInvalidFileName("test-file-name");
    }

    @Test
    void testValidateFileName_incorrectExtension_expectException()
    {
        testCommon_testInvalidFileName("test-file-name.db1");
    }

    @Test
    void testValidateFileName_incorrectCrcExtension_expectException()
    {
        testCommon_testInvalidFileName("crcfile.crc64");
    }

    @Test
    void testValidateFileName_withoutFileName_expectException()
    {
        testCommon_testInvalidFileName("TOC.txt");
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "valid-snapshot-name", "valid.snapshot.name", "valid_snapshot_name",
                             "valid+snapshot+name", "valid..snapshot..name", "valid1snapshot2name",
                             "snap.2026-05-20" })
    void testValidateSnapshotName_validSnapshotNames_expectNoException(String name)
    {
        instance.validateSnapshotName(name);
    }

    @Test
    void testValidateSnapshotName_lengthLimit_expectNoException()
    {
        instance.validateSnapshotName("a".repeat(255));
    }

    @Test
    void testValidateSnapshotName_snapshotNameWithSlash_expectException()
    {
        String testSnapName = "valid" + '/' + "snapshotname";
        assertThatExceptionOfType(CassandraInputException.class).isThrownBy(() -> instance.validateSnapshotName(testSnapName))
                                                                .withMessage("Invalid characters in snapshot name: " + testSnapName);
    }

    @Test
    void testValidateSnapshotName_snapshotNameWithNullChar_expectException()
    {
        String testSnapName = "valid" + '\0' + "snapshotname";
        assertThatExceptionOfType(CassandraInputException.class).isThrownBy(() -> instance.validateSnapshotName(testSnapName))
                                                                .withMessage("Invalid characters in snapshot name: " + testSnapName);
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { ".", ".." })
    void testValidateSnapshotName_snapshotNameWithReservedChar_expectException(String reserved)
    {
        assertThatExceptionOfType(CassandraInputException.class).isThrownBy(() -> instance.validateSnapshotName(reserved))
                                                                .withMessage("Snapshot name '" + reserved + "' is reserved");
    }

    @Test
    void testValidateSnapshotName_snapshotNameExceedsLength_expectException()
    {
        assertThatThrownBy(() -> instance.validateSnapshotName("a".repeat(256)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching("snapshot name must not be more than 255 characters long \\(got 256 characters for.*|" +
                            "Invalid pattern for snapshot name: .*");
    }

    @Test
    void testValidateTableIdIsNull()
    {
        NullPointerException npe = Assertions.assertThrows(NullPointerException.class, () -> instance.validateTableId(null));
        assertThat(npe.getMessage()).isEqualTo("tableId must not be null");
    }

    @ParameterizedTest
    @ValueSource(strings = { "a", "abc", "abc124", "fff1234567890", "53464aa75e6b3d8a84c4e87abbdcbbef" })
    void testValidateTableId(String tableId)
    {
        instance.validateTableId(tableId);
    }

    @Test
    void testTableIdExceedsLengthLimit()
    {
        IllegalArgumentException iae = Assertions.assertThrows(IllegalArgumentException.class,
                                                               () -> instance.validateTableId("53464aa75e6b3d8a84c4e87abbdcbbefa"));
        assertThat(iae.getMessage()).isEqualTo("tableId cannot be longer than 32 characters");
    }

    @ParameterizedTest
    @ValueSource(strings = { "g", "--", "abc-124", "z", "x", "xax" })
    void testInvalidTableId(String tableId)
    {
        assertThatExceptionOfType(CassandraInputException.class).isThrownBy(() -> instance.validateTableId(tableId))
                                                                .withMessage("Invalid characters in table id: " + tableId);
    }

    @Test
    void failsOnNullComponentName()
    {
        assertThatNullPointerException().isThrownBy(() -> instance.validateComponentName(null))
                                        .withMessage("componentName must not be null");
    }

    @Test
    void failsOnNullRestrictedComponentName()
    {
        assertThatNullPointerException().isThrownBy(() -> instance.validateRestrictedComponentName(null))
                                        .withMessage("componentName must not be null");
    }

    @ParameterizedTest
    @ValueSource(strings = { "nb-35460-big-Filter.db", "nb-35460-big-Index.db",
                             "nb-35460-big-Summary.db", "nb-35460-big-Digest.crc32",
                             "nb-35460-big-Data.db", "nb-35460-big-CompressionInfo.db",
                             "nb-35460-big-TOC.txt", "nb-35460-big-Statistics.db" })
    void testValidComponentNameValidation(String componentName)
    {
        assertThat(instance.validateComponentName(componentName)).isEqualTo(componentName);
    }

    @ParameterizedTest
    @ValueSource(strings = { "ks_name-table_name-nb-35460-big-Filter.db", "ks_name-table_name-nb-35460-big-Index.db",
                             "ks_name-table_name-nb-35460-big-Summary.db", "ks_name-table_name-nb-35460-big-Data.db",
                             "ks_name-table_name-nb-35460-big-CompressionInfo.db", "ks_name-table_name-nb-35460-big-TOC.txt",
                             "ks_name-table_name-nb-35460-big-Statistics.db" })
    void testValidRestrictedComponentNameValidation(String componentName)
    {
        assertThat(instance.validateRestrictedComponentName(componentName)).isEqualTo(componentName);
    }

    @ParameterizedTest
    @ValueSource(strings = { "@nb-35460-big-Filter.db", "nb-35460-big-Index.not-valid-ending",
                             "nb-35460-big-duplicate-db.db.db", "nb*35460-big-Digest.crc32",
                             "nb-35460-big-Data-no-dot-db" })
    void failsOnInvalidComponentName(String componentName)
    {
        assertThatExceptionOfType(CassandraInputException.class)
        .isThrownBy(() -> instance.validateComponentName(componentName))
        .withMessage("Invalid component name: " + componentName);
    }

    @ParameterizedTest
    @ValueSource(strings = { "@nb-35460-big-Filter.db", "nb-35460-big-Index.not-valid-ending",
                             "nb-35460-big-Summary.db.db", "nb-35460-big-Digest.crc32",
                             "nb*35460-big-Data.db", "nb-35460-big-CompressionInfo-no-dot-db" })
    void failsOnInvalidRestrictedComponentName(String componentName)
    {
        assertThatExceptionOfType(CassandraInputException.class)
        .isThrownBy(() -> instance.validateRestrictedComponentName(componentName))
        .withMessage("Invalid component name: " + componentName);
    }
}
