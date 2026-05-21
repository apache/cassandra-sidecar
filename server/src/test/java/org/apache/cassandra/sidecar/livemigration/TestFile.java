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
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo;
import org.apache.cassandra.sidecar.common.response.InstanceFileInfo.FileType;
import org.apache.cassandra.sidecar.common.response.InstanceFilesListResponse;
import org.apache.cassandra.sidecar.handlers.livemigration.LiveMigrationDirType;
import org.apache.cassandra.sidecar.utils.DigestAlgorithm;
import org.apache.cassandra.sidecar.utils.TestFileUtils;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.LIVE_MIGRATION_FILES_ROUTE;

class TestFile
{
    public static final int DIR_FILE_SIZE = -1;
    final LiveMigrationDirType dirType;
    final int dirIndex;
    final String relativePath;
    final int size;
    final long lastModifiedTime;

    public TestFile(LiveMigrationDirType dirType, int dirIndex, String relativePath, int size, long lastModifiedTime)
    {
        this.dirType = dirType;
        this.dirIndex = dirIndex;
        this.relativePath = relativePath;
        this.size = size;
        this.lastModifiedTime = lastModifiedTime;
    }

    /**
     * Converts a list of TestFile objects to a list of InstanceFileInfo objects.
     */
    static List<InstanceFileInfo> getInstanceFileInfo(List<TestFile> testFiles)
    {
        return testFiles.stream().map(TestFile::getInstanceFileInfo).collect(Collectors.toList());
    }

    /**
     * Converts a list of TestFile objects to an InstanceFilesListResponse.
     */
    static InstanceFilesListResponse getInstanceFilesListResponse(List<TestFile> testFiles)
    {
        return new InstanceFilesListResponse(getInstanceFileInfo(testFiles));
    }

    /**
     * Returns the InstanceFileInfo representation of this test file.
     */
    InstanceFileInfo getInstanceFileInfo()
    {
        return new InstanceFileInfo(getFileUrl(), size, getFileType(), lastModifiedTime);
    }

    /**
     * Returns the URL path for this file in the live migration API.
     */
    String getFileUrl()
    {
        return LIVE_MIGRATION_FILES_ROUTE + "/" + dirType.dirType + "/" + dirIndex + "/" + relativePath;
    }

    /**
     * Returns the file type (DIRECTORY or FILE) based on the size field.
     */
    FileType getFileType()
    {
        return size == DIR_FILE_SIZE ? FileType.DIRECTORY : FileType.FILE;
    }

    /**
     * Returns the absolute file path given a storage directory.
     */
    String getFilePath(String storageDir)
    {
        return storageDir + "/" + dirType.dirType + "/" + relativePath;
    }

    /**
     * Creates the file or directory represented by this TestFile on the filesystem.
     */
    void createFile(InstanceMetadata instanceMetadata) throws IOException
    {
        Path path = LiveMigrationInstanceMetadataUtil.localPath(getFileUrl(), instanceMetadata);
        if (getFileType() == FileType.DIRECTORY)
        {
            Files.createDirectories(path);
            return;
        }
        TestFileUtils.createFile(path.toAbsolutePath().toString(), size, lastModifiedTime);
    }

    /**
     * Deletes the file or directory represented by this TestFile from the filesystem.
     */
    void deleteFile(InstanceMetadata instanceMetadata) throws IOException
    {
        Path path = LiveMigrationInstanceMetadataUtil.localPath(getFileUrl(), instanceMetadata);
        Files.deleteIfExists(path);
    }

    /**
     * Calculates and returns the digest of this file using the provided algorithm supplier.
     * Returns null for directories as they are not included in digest comparisons.
     */
    String digest(InstanceMetadata instanceMetadata, Supplier<DigestAlgorithm> digestAlgorithmSupplier) throws IOException
    {
        if (getFileType() == FileType.DIRECTORY)
        {
            // directories are not considered for digest comparison, hence returning null
            return null;
        }
        Path path = LiveMigrationInstanceMetadataUtil.localPath(getFileUrl(), instanceMetadata);
        byte[] bytes = Files.readAllBytes(path);

        DigestAlgorithm digestAlgorithm = digestAlgorithmSupplier.get();
        digestAlgorithm.update(bytes, 0, bytes.length);
        return digestAlgorithm.digest();
    }
}
