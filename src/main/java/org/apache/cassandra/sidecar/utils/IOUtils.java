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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Utilities for IO operations
 */
public class IOUtils
{
    /**
     * Returns the {@link StandardCharsets#UTF_8} String read from the provided {@code resource}.
     *
     * @param resource the resource to read
     * @return the {@link StandardCharsets#UTF_8} String read from the provided {@code resource}
     * @throws IOException when an error occurs during reading the resource or decoding the resource to {@code UTF-8}
     */
    public static String readFully(String resource) throws IOException
    {
        return readFully(IOUtils.class.getResourceAsStream(resource));
    }

    /**
     * Returns the {@link StandardCharsets#UTF_8} String read from the provided {@link InputStream inputStream}.
     *
     * @param inputStream the input stream to read from
     * @return the {@link StandardCharsets#UTF_8} String read from the provided {@link InputStream inputStream}
     * @throws IOException when an error occurs during reading the input stream or decoding the resource to
     *                     {@code UTF-8}
     */
    public static String readFully(InputStream inputStream) throws IOException
    {
        try (InputStream input = inputStream;
             ByteArrayOutputStream output = new ByteArrayOutputStream())
        {
            int length;
            byte[] buffer = new byte[32];
            while ((length = input.read(buffer)) >= 0)
            {
                output.write(buffer, 0, length);
            }
            return output.toString(StandardCharsets.UTF_8.name());
        }
    }
}
