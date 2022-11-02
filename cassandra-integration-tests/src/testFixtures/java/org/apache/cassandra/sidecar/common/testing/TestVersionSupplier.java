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

package org.apache.cassandra.sidecar.common.testing;

import java.util.stream.Stream;

import org.apache.cassandra.sidecar.cassandra40.Cassandra40Factory;

/**
 * Generates the list of versions we're going to test against.
 * Depending on future releases, we may end up running the same module (Cassandra40 for example) against multiple
 * versions of Cassandra.  This may be due to releases that don't add new features that would affect the sidecar,
 * but we still want to test those versions specifically to avoid the chance of regressions.
 *
 * <p>At the moment, it's returning a hard coded list.  We could / should probably load this from a configuration and make
 * it possible to override it, so teams that customize C* can run and test their own implementation
 *
 * <p>Ideally, we'd probably have concurrent runs of the test infrastructure each running tests against one specific
 * version of C*, but we don't need that yet given we only have one version.
 */
public class TestVersionSupplier
{
    Stream<TestVersion> getTestVersions()
    {
        return Stream.of(new TestVersion("4.0.0", new Cassandra40Factory(), "cassandra:4.0"));
    }

}
