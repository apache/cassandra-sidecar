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

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.handler.HttpException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test validation methods.
 */
public class CassandraInputValidatorTest
{
    CassandraInputValidator instance;

    @BeforeEach
    void setup()
    {
        instance = new CassandraInputValidator();
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "test_table_name", "\"test_table_name\"", "testTableName", "\"testTableName\"", "a_",
                             "\"cycling\"", "\"Helmets\"", "\"mIxEd_cAsE\"", "a8", "a", "\"8a\"",
                             "\"_must_begin_with_alphabetic_unless_quoted_p\"" })
    public void testValidTableNameValidation(String tableName)
    {
        instance.validateTableName(tableName);
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "", "test table", "_must_begin_with_alphabetic", "dash-is-not-allowed", "\"\"", "\"",
                             "\"inv@lid_chars\"", "test:table_name", "test-table$name", "8a", "testTable/Name" })
    public void failsWithInvalidTableName(String tableName)
    {
        HttpException httpEx = Assertions.assertThrows(HttpException.class,
                                                       () -> instance.validateTableName(tableName));
        assertThat(httpEx.getStatusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        assertThat(httpEx.getPayload()).isEqualTo("Invalid characters in table name: " + tableName);
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "SystemViews", "system_views_test", "\"keyspace\"", "\"cycling\"", "\"Cycling\"",
                             "\"mIxEd_cAsE\"", "a8", "a", "a_", "\"_a\"" })
    public void testValidKeyspaceValidation(String keyspace)
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
    public void failsWithForbiddenKeyspace(String keyspace)
    {
        HttpException httpEx = Assertions.assertThrows(HttpException.class,
                                                       () -> instance.validateKeyspaceName(keyspace));
        assertThat(httpEx.getStatusCode()).isEqualTo(HttpResponseStatus.FORBIDDEN.code());
        assertThat(httpEx.getPayload()).isEqualTo("Forbidden keyspace: " + keyspace);
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "", "test keyspace", "_cycling", "dash-is-not-allowed", "\"\"", "\"",
                             "\"inv@lid_chars\"", "8a" })
    public void failsWithInvalidKeyspaceName(String keyspace)
    {
        HttpException httpEx = Assertions.assertThrows(HttpException.class,
                                                       () -> instance.validateKeyspaceName(keyspace));
        assertThat(httpEx.getStatusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        assertThat(httpEx.getPayload()).isEqualTo("Invalid characters in keyspace: " + keyspace);
    }

    @Test
    public void testValidateFileName_validFileNames_expectNoException()
    {
        instance.validateComponentName("test-file-name.db");
        instance.validateComponentName("test_file_name.json");
        instance.validateComponentName("testFileName.cql");
        instance.validateComponentName("t_TOC.txt");
        instance.validateComponentName("crcfile.crc32");
    }

    private void testCommon_testInvalidFileName(String testFileName)
    {
        HttpException httpEx = Assertions.assertThrows(HttpException.class,
                                                       () -> instance.validateComponentName(testFileName));
        assertThat(httpEx.getStatusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        assertThat(httpEx.getPayload()).isEqualTo("Invalid component name: " + testFileName);
    }

    @Test
    public void testValidateFileName_withoutExtension_expectException()
    {
        testCommon_testInvalidFileName("test-file-name");
    }

    @Test
    public void testValidateFileName_incorrectExtension_expectException()
    {
        testCommon_testInvalidFileName("test-file-name.db1");
    }

    @Test
    public void testValidateFileName_incorrectCrcExtension_expectException()
    {
        testCommon_testInvalidFileName("crcfile.crc64");
    }

    @Test
    public void testValidateFileName_withoutFileName_expectException()
    {
        testCommon_testInvalidFileName("TOC.txt");
    }


    @Test
    public void testValidateSnapshotName_validSnapshotNames_expectNoException()
    {
        instance.validateSnapshotName("valid-snapshot-name");
        instance.validateSnapshotName("valid\\snapshot\\name");
        instance.validateSnapshotName("valid:snapshot:name");
        instance.validateSnapshotName("valid$snapshot$name");
        instance.validateSnapshotName("valid snapshot name");
    }

    @Test
    public void testValidateSnapshotName_snapshotNameWithSlash_expectException()
    {
        String testSnapName = "valid" + '/' + "snapshotname";
        HttpException httpEx = Assertions.assertThrows(HttpException.class,
                                                       () -> instance.validateSnapshotName(testSnapName));
        assertThat(httpEx.getStatusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        assertThat(httpEx.getPayload()).isEqualTo("Invalid characters in snapshot name: " + testSnapName);
    }

    @Test
    public void testValidateSnapshotName_snapshotNameWithNullChar_expectException()
    {
        String testSnapName = "valid" + '\0' + "snapshotname";
        HttpException httpEx = Assertions.assertThrows(HttpException.class,
                                                       () -> instance.validateSnapshotName(testSnapName));
        assertThat(httpEx.getStatusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        assertThat(httpEx.getPayload()).isEqualTo("Invalid characters in snapshot name: " + testSnapName);
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
        HttpException httpEx = Assertions.assertThrows(HttpException.class,
                                                       () -> instance.validateTableId(tableId));
        assertThat(httpEx.getStatusCode()).isEqualTo(HttpResponseStatus.BAD_REQUEST.code());
        assertThat(httpEx.getPayload()).isEqualTo("Invalid characters in table id: " + tableId);
    }
}
