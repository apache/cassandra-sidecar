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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.cassandra.sidecar.exceptions.RestoreJobException;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.utils.DigestAlgorithm;
import org.apache.cassandra.sidecar.utils.DigestAlgorithmProvider;

/**
 * Utilities that only makes sense in the context of restore jobs.
 *
 * Note: Avoid using it in the other scenarios.
 *
 */
@Singleton
public class RestoreJobUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreJobUtil.class);
    private static final int KB_512 = 512 * 1024;
    // it is part of upload id and get validated by
    // org.apache.cassandra.sidecar.utils.SSTableUploadsPathBuilder.UPLOAD_ID_PATTERN
    private static final String RESTORE_JOB_PREFIX = "c0ffee-";
    private static final int RESTORE_JOB_PREFIX_LEN = RESTORE_JOB_PREFIX.length();
    private static final int RESTORE_JOB_DEFAULT_HASH_SEED = 0;

    private DigestAlgorithmProvider digestAlgorithmProvider;

    @Inject
    public RestoreJobUtil(@Named("xxhash32") DigestAlgorithmProvider digestAlgorithmProvider)
    {
        this.digestAlgorithmProvider = digestAlgorithmProvider;
    }

    /**
     * Unzip a restore slice zip
     * @param zipFile source zip file
     * @param targetDir directory to keep the unzipped files
     * @throws IOException I/O exceptions during unzip
     * @throws RestoreJobException the zip file is malicious
     */
    public static void unzip(File zipFile, File targetDir) throws IOException, RestoreJobException
    {
        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFile.toPath())))
        {
            ZipEntry zipEntry = zis.getNextEntry();

            while (zipEntry != null)
            {
                // Encounters a directory inside the zip file
                // It is not expected. The zip file should have the directory depth of 1.
                // The reason of not using isDirectory(): in test, it gets 'dir/file' directly, instead of 'dir/'
                if (zipEntry.getName().contains(File.separator))
                {
                    throw new RestoreJobFatalException("Unexpected directory in slice zip file. File: " + zipFile);
                }

                File targetFile = newProtectedTargetFile(zipEntry, targetDir);
                Files.copy(zis, targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
        }
    }

    /**
     * Resolve a prefixed job id for path
     * @param jobId restore job id
     * @return prefixed job id string
     */
    public static String prefixedJobId(UUID jobId)
    {
        return RESTORE_JOB_PREFIX + jobId;
    }

    public static String generateUniqueUploadId(UUID jobId, String sliceId)
    {
        return prefixedJobId(jobId) + '-' + sliceId + '-' + ThreadLocalRandom.current().nextInt(10000);
    }

    /**
     * Extract the timestamp from the restore job directory name.
     * @param fileName directory file name
     * @return unix timestamp epoch; otherwise, return -1 if no timestamp can be extracted
     */
    public static long timestampFromRestoreJobDir(String fileName)
    {
        if (!fileName.startsWith(RESTORE_JOB_PREFIX))
            return -1;

        try
        {
            UUID id = UUID.fromString(fileName.substring(RESTORE_JOB_PREFIX_LEN));
            return UUIDs.unixTimestamp(id);
        }
        catch (IllegalArgumentException e)
        {
            return -1;
        }
    }

    /**
     * Cleans given directory path without deleting it
     * @param path directory path to be cleaned
     * @throws IOException when I/O error occurs
     */
    public static void cleanDirectory(Path path) throws IOException
    {
        if (!Files.isDirectory(path) || !Files.exists(path))
        {
            LOGGER.warn("Clean directory API called for non existent directory: {}", path);
            return;
        }

        try (Stream<Path> contents = Files.list(path))
        {
            contents.forEach(filePath -> {
                try
                {
                    if (Files.isDirectory(filePath))
                    {
                        cleanDirectory(filePath);
                    }
                    Files.delete(filePath);
                }
                catch (IOException e)
                {
                    LOGGER.error("Unexpected error occurred while cleaning directory {}, ", path, e);
                    throw new RuntimeException(e);
                }
            });
        }
    }

    /**
     * Create a file that is protected from zip slip attack (https://security.snyk.io/research/zip-slip-vulnerability)
     * @param zipEntry zip entry to be extracted
     * @param targetDir directory to keep the unzipped files
     * @return a new file
     * @throws IOException failed to resolving path
     * @throws RestoreJobException if the zip file is malicious
     */
    private static File newProtectedTargetFile(ZipEntry zipEntry, File targetDir)
    throws IOException, RestoreJobException
    {
        File targetFile = new File(targetDir, zipEntry.getName());

        // Normalize the paths of both target dir and file
        String targetDirPath = targetDir.getCanonicalPath();
        String targetFilePath = targetFile.getCanonicalPath();

        if (!targetFilePath.startsWith(targetDirPath))
        {
            throw new RestoreJobException("Bad zip entry: " + zipEntry.getName());
        }

        return targetFile;
    }

    /**
     * @param file the file to use to perform the checksum
     * @return the checksum hex string of the file's content. XXHash32 is employed as the hash algorithm.
     */
    public String checksum(File file) throws IOException
    {
        return checksum(file, RESTORE_JOB_DEFAULT_HASH_SEED);
    }

    /**
     * @param file the file to use to perform the checksum
     * @param seed the seed to use for the hasher
     * @return the checksum hex string of the file's content. XXHash32 is employed as the hash algorithm.
     */
    public String checksum(File file, int seed) throws IOException
    {
        try (InputStream fis = Files.newInputStream(file.toPath()))
        {
            try (DigestAlgorithm digestAlgorithm = digestAlgorithmProvider.get(seed))
            {
                byte[] buffer = new byte[KB_512];
                int len;
                while ((len = fis.read(buffer)) != -1)
                {
                    digestAlgorithm.update(buffer, 0, len);
                }
                return digestAlgorithm.digest();
            }
        }
    }
}
