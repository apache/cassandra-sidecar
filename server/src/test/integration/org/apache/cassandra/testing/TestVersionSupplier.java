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

package org.apache.cassandra.testing;

import java.util.Arrays;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates the list of versions we're going to test against.
 * We will run the same module (trunk for example) against multiple versions of Cassandra.
 * This is due to releases that don't add new features that would affect the sidecar,
 * but we still want to test those versions specifically to avoid the chance of regressions.
 *
 * <p>Ideally, we'd probably have concurrent runs of the test infrastructure each running tests against one specific
 * version of C*, but this will require some additional work in the dtest framework so for now we run one at a time.
 */
public class TestVersionSupplier
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TestVersionSupplier.class);

    private TestVersionSupplier()
    {
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    public static Stream<TestVersion> testVersions()
    {
        // By default, we test 2 versions that will exercise oldest and newest supported versions
        String versions = System.getProperty("cassandra.sidecar.versions_to_test", "4.0,5.1");
        LOGGER.info("Testing with versions={}", versions);
        return Arrays.stream(versions.split(",")).map(String::trim).map(TestVersion::new);
    }
}
