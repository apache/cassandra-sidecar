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

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.handler.HttpException;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

    private void testCommon_invalidCharacters(String testName)
    {
        HttpException httpEx = Assertions.assertThrows(HttpException.class, () -> instance.validateTableName(testName));
        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), httpEx.getStatusCode());
        assertEquals("Invalid characters in table name: " + testName, httpEx.getPayload());
    }

    @Test
    public void testValidateCharacters_validParams_expectNoException()
    {
        instance.validateTableName("test_table_name");
        instance.validateTableName("test-table-name");
        instance.validateTableName("testTableName");
    }

    @Test
    public void testValidateCharacters_paramWithColon_expectException()
    {
        testCommon_invalidCharacters("test:table_name");
    }

    @Test
    public void testValidateCharacters_paramWithDollar_expectException()
    {
        testCommon_invalidCharacters("test-table$name");
    }

    @Test
    public void testValidateCharacters_paramsWithSlash_expectException()
    {
        testCommon_invalidCharacters("testTable/Name");
    }


    @Test
    public void testValidateKeyspaceName_validKeyspaceNames_expectNoException()
    {
        instance.validateKeyspaceName("system-views");
        instance.validateKeyspaceName("SystemViews");
        instance.validateKeyspaceName("system_views_test");
    }

    @Test
    public void testValidateKeyspaceName_forbiddenKeyspaceName_expectException()
    {
        String testKS = "system_views";
        HttpException httpEx = Assertions.assertThrows(HttpException.class,
                                                       () -> instance.validateKeyspaceName(testKS));
        assertEquals(HttpResponseStatus.FORBIDDEN.code(), httpEx.getStatusCode());
        assertEquals("Forbidden keyspace: " + testKS, httpEx.getPayload());
    }

    @Test
    public void testValidateKeyspaceName_keyspaceNameWithSpace_expectException()
    {
        String testKS = "test keyspace";
        HttpException httpEx = Assertions.assertThrows(HttpException.class,
                                                       () -> instance.validateKeyspaceName(testKS));
        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), httpEx.getStatusCode());
        assertEquals("Invalid characters in keyspace: " + testKS, httpEx.getPayload());
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
        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), httpEx.getStatusCode());
        assertEquals("Invalid component name: " + testFileName, httpEx.getPayload());
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
        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), httpEx.getStatusCode());
        assertEquals("Invalid characters in snapshot name: " + testSnapName, httpEx.getPayload());
    }

    @Test
    public void testValidateSnapshotName_snapshotNameWithNullChar_expectException()
    {
        String testSnapName = "valid" + '\0' + "snapshotname";
        HttpException httpEx = Assertions.assertThrows(HttpException.class,
                                                       () -> instance.validateSnapshotName(testSnapName));
        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), httpEx.getStatusCode());
        assertEquals("Invalid characters in snapshot name: " + testSnapName, httpEx.getPayload());
    }
}
