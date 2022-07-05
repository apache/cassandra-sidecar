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

package org.apache.cassandra.sidecar.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.common.utils.ValidationUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test validation methods.
 */
public class ValidationUtilsTest
{

    private void testCommon_invalidCharacters(String testName)
    {
        HttpException httpEx = Assertions.assertThrows(HttpException.class, () ->
        {
            ValidationUtils.validateTableName(testName);
        });
        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), httpEx.getStatusCode());
        assertEquals("Invalid characters in table name: " + testName, httpEx.getPayload());
    }

    @Test
    public void testValidateCharacters_validParams_expectNoException()
    {
        ValidationUtils.validateTableName("test_table_name");
        ValidationUtils.validateTableName("test-table-name");
        ValidationUtils.validateTableName("testTableName");
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
        ValidationUtils.validateKeyspaceName("system-views");
        ValidationUtils.validateKeyspaceName("SystemViews");
        ValidationUtils.validateKeyspaceName("system_views_test");
    }

    @Test
    public void testValidateKeyspaceName_forbiddenKeyspaceName_expectException()
    {
        String testKS = "system_views";
        HttpException httpEx = Assertions.assertThrows(HttpException.class, () ->
        {
            ValidationUtils.validateKeyspaceName(testKS);
        });
        assertEquals(HttpResponseStatus.FORBIDDEN.code(), httpEx.getStatusCode());
        assertEquals("Forbidden keyspace: " + testKS, httpEx.getPayload());
    }

    @Test
    public void testValidateKeyspaceName_keyspaceNameWithSpace_expectException()
    {
        String testKS = "test keyspace";
        HttpException httpEx = Assertions.assertThrows(HttpException.class, () ->
        {
            ValidationUtils.validateKeyspaceName(testKS);
        });
        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), httpEx.getStatusCode());
        assertEquals("Invalid characters in keyspace: " + testKS, httpEx.getPayload());
    }


    @Test
    public void testValidateFileName_validFileNames_expectNoException()
    {
        ValidationUtils.validateComponentName("test-file-name.db");
        ValidationUtils.validateComponentName("test_file_name.json");
        ValidationUtils.validateComponentName("testFileName.cql");
        ValidationUtils.validateComponentName("t_TOC.txt");
        ValidationUtils.validateComponentName("crcfile.crc32");
    }

    private void testCommon_testInvalidFileName(String testFileName)
    {
        HttpException httpEx = Assertions.assertThrows(HttpException.class, () ->
        {
            ValidationUtils.validateComponentName(testFileName);
        });
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
        ValidationUtils.validateSnapshotName("valid-snapshot-name");
        ValidationUtils.validateSnapshotName("valid\\snapshot\\name");
        ValidationUtils.validateSnapshotName("valid:snapshot:name");
        ValidationUtils.validateSnapshotName("valid$snapshot$name");
        ValidationUtils.validateSnapshotName("valid snapshot name");
    }

    @Test
    public void testValidateSnapshotName_snapshotNameWithSlash_expectException()
    {
        String testSnapName = "valid" + '/' + "snapshotname";
        HttpException httpEx = Assertions.assertThrows(HttpException.class, () ->
        {
            ValidationUtils.validateSnapshotName(testSnapName);
        });
        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), httpEx.getStatusCode());
        assertEquals("Invalid characters in snapshot name: " + testSnapName, httpEx.getPayload());
    }

    @Test
    public void testValidateSnapshotName_snapshotNameWithNullChar_expectException()
    {
        String testSnapName = "valid" + '\0' + "snapshotname";
        HttpException httpEx = Assertions.assertThrows(HttpException.class, () ->
        {
            ValidationUtils.validateSnapshotName(testSnapName);
        });
        assertEquals(HttpResponseStatus.BAD_REQUEST.code(), httpEx.getStatusCode());
        assertEquals("Invalid characters in snapshot name: " + testSnapName, httpEx.getPayload());
    }

}
