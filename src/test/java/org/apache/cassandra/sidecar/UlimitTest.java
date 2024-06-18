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

package org.apache.cassandra.sidecar;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to ensure that the max file descriptors is large enough to build Cassandra Sidecar
 */
@EnabledOnOs({OS.MAC})
public class UlimitTest
{

    /**
     * Runs a test to ensure that the Max Files isn't too low
     * @throws Exception when maxFD isn't large enough
     */
    @Test
    void ensureUlimitMaxFilesIsNotTooLow() throws Exception
    {
        Process p = new ProcessBuilder("ulimit", "-n").start();
        p.waitFor();
        InputStreamReader inputStreamReader = new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(inputStreamReader);
        String line = reader.readLine();
        long maxFD = Long.parseLong(line);
        assertThat(maxFD).isGreaterThan(10240);
        inputStreamReader.close();
        reader.close();
    }
}
