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

package org.apache.cassandra.sidecar.livemigration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.cassandra.sidecar.common.response.InstanceFileInfo;

import static org.apache.cassandra.sidecar.livemigration.InstanceFileInfoTestUtil.findInstanceFileInfo;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.createDirectory;
import static org.apache.cassandra.sidecar.utils.TestFileUtils.createFile;
import static org.assertj.core.api.Assertions.assertThat;

class DirVisitorTest
{
    @TempDir
    Path tempDir;

    @SuppressWarnings("DataFlowIssue")
    @Test
    void testFiles() throws IOException
    {
        Path dataDir = Paths.get(tempDir.toString(), "data");
        Files.createDirectories(dataDir);

        String homeDir = dataDir.toString();
        createDirectory(homeDir, "ks1", "t1");
        // creating a test file with length 4
        createFile("abcd", homeDir, "ks1", "t1", "data1.db");
        // creating a test file with length 5
        createFile("efghi", homeDir, "ks1", "t1", "data2.db");
        int homeDirIndex = 3;
        String pathPrefix = "/DUMMY_PREFIX/" + homeDirIndex;

        DirVisitor dirVisitor = new DirVisitor(homeDir, pathPrefix, Collections.emptyList(), Collections.emptyList());
        List<InstanceFileInfo> files = dirVisitor.files();
        assertThat(files.size()).isEqualTo(4); // Two directories and two files

        assertThat(findInstanceFileInfo(files, pathPrefix + "/ks1")).isNotNull();
        assertThat(findInstanceFileInfo(files, pathPrefix + "/ks1").size)
        .isEqualTo(-1);

        assertThat(findInstanceFileInfo(files, pathPrefix + "/ks1/t1")).isNotNull();
        assertThat(findInstanceFileInfo(files, pathPrefix + "/ks1/t1").size)
        .isEqualTo(-1);

        assertThat(findInstanceFileInfo(files, pathPrefix + "/ks1/t1/data1.db")).isNotNull();
        assertThat(findInstanceFileInfo(files, pathPrefix + "/ks1/t1/data1.db").size)
        .isEqualTo(4);

        assertThat(findInstanceFileInfo(files, pathPrefix + "/ks1/t1/data2.db")).isNotNull();
        assertThat(findInstanceFileInfo(files, pathPrefix + "/ks1/t1/data2.db").size)
        .isEqualTo(5);
    }
}
