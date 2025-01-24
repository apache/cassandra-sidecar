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

package org.apache.cassandra.sidecar.common.server.data;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus.ABORTED;
import static org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus.CREATED;
import static org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus.DISCARDED;
import static org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus.FAILED;
import static org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus.STAGED;
import static org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RestoreRangeStatusTest
{
    @Test
    void testStatusAdvancing()
    {
        assertAdvanceTo(CREATED, FAILED);
        assertAdvanceTo(CREATED, ABORTED);
        assertAdvanceTo(CREATED, DISCARDED);
        assertAdvanceTo(CREATED, STAGED);
        assertAdvanceTo(STAGED, FAILED);
        assertAdvanceTo(STAGED, ABORTED);
        assertAdvanceTo(STAGED, DISCARDED);
        assertAdvanceTo(STAGED, SUCCEEDED);
    }

    @ParameterizedTest(name = "{index}: {0} -> {1}")
    @MethodSource("invalidStatusAdvancingSource")
    void testInvalidStatusAdvancing(RestoreRangeStatus from, RestoreRangeStatus to)
    {
        String commonErrorMsg = from + " status can only advance to one of the follow statuses";

        assertThatThrownBy(() -> from.advanceTo(to)).isExactlyInstanceOf(IllegalArgumentException.class)
                                                    .hasNoCause()
                                                    .hasMessageContaining(commonErrorMsg);
    }

    public static Stream<Arguments> invalidStatusAdvancingSource()
    {
        return Stream.of(Arguments.of(STAGED, CREATED),
                         Arguments.of(CREATED, SUCCEEDED),
                         Arguments.of(STAGED, STAGED),
                         Arguments.of(SUCCEEDED, FAILED),
                         Arguments.of(FAILED, SUCCEEDED),
                         Arguments.of(FAILED, ABORTED),
                         Arguments.of(DISCARDED, CREATED),
                         Arguments.of(DISCARDED, STAGED),
                         Arguments.of(DISCARDED, SUCCEEDED));
    }

    private void assertAdvanceTo(RestoreRangeStatus from, RestoreRangeStatus to)
    {
        assertThat(from.advanceTo(to)).isEqualTo(to);
    }
}
