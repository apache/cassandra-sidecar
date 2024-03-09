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

package org.apache.cassandra.sidecar.restore;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.utils.XXHash32Provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RestoreJobUtilTest
{
    @TempDir
    File testDirRoot;

    @Test
    void testChecksum() throws IOException
    {
        File file = new File(testDirRoot, "checksum.txt");
        Files.write(file.toPath(), "XXHash32 is employed as the hash algorithm".getBytes(StandardCharsets.UTF_8));
        RestoreJobUtil util = new RestoreJobUtil(new XXHash32Provider());
        String checksum = util.checksum(file);
        assertThat(checksum)
        .describedAs("Hasher should return 32 bits checksum == 4 characters")
        .hasSize(8)
        .isEqualTo("a0051d07"); // hash of "XXHash32 is employed as the hash algorithm"
    }

    @Test
    void testUnzip() throws Exception
    {
        File targetDir = new File(testDirRoot, "unzipTest");
        assertThat(targetDir.mkdirs()).isTrue();
        File zipFile = new File(getClass().getResource("/test_unzip.zip").toURI());
        RestoreJobUtil.unzip(zipFile, targetDir);
        assertThat(targetDir.list())
        .describedAs("It should unzip the file successfully. The zip contains 3 files named, a, b and c")
        .containsExactlyInAnyOrder("a", "b", "c");
    }

    @Test
    void testUnzipMalformed() throws Exception
    {
        File targetDir = new File(testDirRoot, "unzipMalformedTest");
        assertThat(targetDir.mkdirs()).isTrue();
        File zipFile = new File(getClass().getResource("/test_unzip_malformed.zip").toURI());
        assertThatThrownBy(() -> RestoreJobUtil.unzip(zipFile, targetDir))
        .isInstanceOf(RestoreJobFatalException.class)
        .hasMessage("Unexpected directory in slice zip file. File: " + zipFile);
    }

    @Test
    void testExtractTimestampFromRestoreJobDirName()
    {
        assertThat(RestoreJobUtil.timestampFromRestoreJobDir(RestoreJobUtil.prefixedJobId(UUIDs.timeBased())))
        .isPositive();

        // all below are -1
        assertThat(RestoreJobUtil.timestampFromRestoreJobDir(RestoreJobUtil.prefixedJobId(UUID.randomUUID())))
        .isEqualTo(-1);
        assertThat(RestoreJobUtil.timestampFromRestoreJobDir(RestoreJobUtil.prefixedJobId(UUID.randomUUID())))
        .isEqualTo(-1);
        assertThat(RestoreJobUtil.timestampFromRestoreJobDir("random_file"))
        .isEqualTo(-1);
        assertThat(RestoreJobUtil.timestampFromRestoreJobDir(RestoreJobUtil.prefixedJobId(UUID.randomUUID())
                                                             + "-invalid-uuid"))
        .isEqualTo(-1);
    }
}
