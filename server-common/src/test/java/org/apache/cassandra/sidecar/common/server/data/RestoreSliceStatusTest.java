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

import static org.apache.cassandra.sidecar.common.server.data.RestoreSliceStatus.ABORTED;
import static org.apache.cassandra.sidecar.common.server.data.RestoreSliceStatus.COMMITTING;
import static org.apache.cassandra.sidecar.common.server.data.RestoreSliceStatus.EMPTY;
import static org.apache.cassandra.sidecar.common.server.data.RestoreSliceStatus.FAILED;
import static org.apache.cassandra.sidecar.common.server.data.RestoreSliceStatus.PROCESSING;
import static org.apache.cassandra.sidecar.common.server.data.RestoreSliceStatus.STAGED;
import static org.apache.cassandra.sidecar.common.server.data.RestoreSliceStatus.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RestoreSliceStatusTest
{
    @Test
    void testStatusAdvancing()
    {
        assertAdvanceTo(EMPTY, PROCESSING);
        assertAdvanceTo(EMPTY, FAILED);
        assertAdvanceTo(EMPTY, ABORTED);
        assertAdvanceTo(PROCESSING, STAGED);
        assertAdvanceTo(PROCESSING, FAILED);
        assertAdvanceTo(PROCESSING, ABORTED);
        assertAdvanceTo(STAGED, COMMITTING);
        assertAdvanceTo(STAGED, FAILED);
        assertAdvanceTo(STAGED, ABORTED);
        assertAdvanceTo(COMMITTING, SUCCEEDED);
        assertAdvanceTo(COMMITTING, FAILED);
        assertAdvanceTo(COMMITTING, ABORTED);
    }

    @Test
    void testInvalidStatusAdvancing()
    {
        String commonErrorMsg = "status can only advance to one of the follow statuses";

        Stream
        .of(new RestoreSliceStatus[][]
            { // define test cases of invalid status advancing, e.g. it is invalid to advance from EMPTY to STAGED
              { EMPTY, STAGED },
              { STAGED, EMPTY },
              { EMPTY, COMMITTING },
              { STAGED, SUCCEEDED },
              { COMMITTING, STAGED },
              { STAGED, STAGED },
              { SUCCEEDED, FAILED },
              { FAILED, SUCCEEDED }
            })
        .forEach(testCase -> {
            assertThatThrownBy(() -> testCase[0].advanceTo(testCase[1]))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasNoCause()
            .hasMessageContaining(commonErrorMsg);
        });
    }

    private void assertAdvanceTo(RestoreSliceStatus from, RestoreSliceStatus to)
    {
        assertThat(from.advanceTo(to)).isEqualTo(to);
    }
}
