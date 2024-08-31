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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

/**
 * File utilities for tests
 */
public class TestFileUtils
{
    /**
     * Writes random data to a file with name {@code filename} under the specified {@code directory} with
     * the specified size in bytes.
     *
     * @param directory   the directory where to
     * @param fileName    the name of the desired file to create
     * @param sizeInBytes the size of the files in bytes
     * @return the path of the file that was recently created
     * @throws IOException when file creation or writing to the file fails
     */
    public static Path prepareTestFile(Path directory, String fileName, long sizeInBytes) throws IOException
    {
        Path filePath = directory.resolve(fileName);
        Files.deleteIfExists(filePath);

        byte[] buffer = new byte[1024];
        try (OutputStream outputStream = Files.newOutputStream(filePath))
        {
            int written = 0;
            while (written < sizeInBytes)
            {
                ThreadLocalRandom.current().nextBytes(buffer);
                int toWrite = (int) Math.min(buffer.length, sizeInBytes - written);
                outputStream.write(buffer, 0, toWrite);
                written += toWrite;
            }
        }
        return filePath;
    }
}
