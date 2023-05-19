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

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Tests that CassandraVersion comparisons work correctly
 */
public class SimpleCassandraVersionTest
{
    SimpleCassandraVersion v4 = SimpleCassandraVersion.create(4, 0, 0);
    SimpleCassandraVersion v5 = SimpleCassandraVersion.create(5, 0, 0);

    @Test
    void testNegativeVersionsFail()
    {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> SimpleCassandraVersion.create(-1, 0, 0));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> SimpleCassandraVersion.create(4, -1, 0));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> SimpleCassandraVersion.create(0, 0, -1));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> SimpleCassandraVersion.create("-3.0.0"));
    }

    @Test
    void testNormalParse()
    {
        assertThat(v4.major).isEqualTo(4);
        assertThat(v4.minor).isEqualTo(0);
        assertThat(v5.patch).isEqualTo(0);
    }

    @Test
    void testParseNonZeros()
    {
        SimpleCassandraVersion v6 = SimpleCassandraVersion.create("6.14.13");
        assertThat(v6.major).isEqualTo(6);
        assertThat(v6.minor).isEqualTo(14);
        assertThat(v6.patch).isEqualTo(13);
    }

    @Test
    void testMajorCompare()
    {
        assertThat(v5).isGreaterThan(v4);
    }

    @Test
    void testMinorCompare()
    {
        SimpleCassandraVersion v41 = SimpleCassandraVersion.create(4, 1, 0);
        assertThat(v41).isGreaterThan(v4);
    }

    @Test
    void testBugFixCompare()
    {
        SimpleCassandraVersion v401 = SimpleCassandraVersion.create(4, 0, 1);
        assertThat(v401).isGreaterThan(v4);
    }

    @Test
    void testEqual()
    {
        SimpleCassandraVersion v4Alt = SimpleCassandraVersion.create(4, 0, 0);
        assertThat(v4Alt).isEqualByComparingTo(v4);

    }

    @Test
    void testAlphaParsing()
    {
        SimpleCassandraVersion alpha = SimpleCassandraVersion.create("4.0-alpha4");
        assertThat(alpha).isEqualTo(v4);
        assertThat(alpha.major).isEqualTo(4);
        assertThat(alpha.minor).isEqualTo(0);
        assertThat(alpha.patch).isEqualTo(0);

        String parsed = alpha.toString();
        SimpleCassandraVersion alpha2 = SimpleCassandraVersion.create(parsed);
        assertThat(alpha).isEqualTo(alpha2);
    }

    @Test
    void testGreaterThan()
    {
        assertThat(v5.isGreaterThan(v4)).isTrue();
    }

    @Test
    void testSnapshotBuild()
    {
        SimpleCassandraVersion alpha = SimpleCassandraVersion.create("4.0-alpha5-SNAPSHOT");
        assertThat(alpha.major).isEqualTo(4);
        assertThat(alpha.minor).isEqualTo(0);
        assertThat(alpha.patch).isEqualTo(0);

    }

    @Test
    void testLowerCaseSnapshotBuild()
    {
        SimpleCassandraVersion snapshot = SimpleCassandraVersion.create("4.0.0.0-snapshot");
        assertThat(snapshot.major).isEqualTo(4);
        assertThat(snapshot.minor).isEqualTo(0);
        assertThat(snapshot.patch).isEqualTo(0);
    }

}
