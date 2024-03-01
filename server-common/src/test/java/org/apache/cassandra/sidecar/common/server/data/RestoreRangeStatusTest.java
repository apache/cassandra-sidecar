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

import static org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus.ABORTED;
import static org.apache.cassandra.sidecar.common.server.data.RestoreRangeStatus.CREATED;
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
        assertAdvanceTo(CREATED, STAGED);
        assertAdvanceTo(STAGED, FAILED);
        assertAdvanceTo(STAGED, ABORTED);
        assertAdvanceTo(STAGED, SUCCEEDED);
    }

    @Test
    void testInvalidStatusAdvancing()
    {
        String commonErrorMsg = "status can only advance to one of the follow statuses";

        Stream
        .of(new RestoreRangeStatus[][]
            { // define test cases of invalid status advancing, e.g. it is invalid to advance from EMPTY to STAGED
              { STAGED, CREATED },
              { CREATED, SUCCEEDED },
              { STAGED, STAGED },
              { SUCCEEDED, FAILED },
              { FAILED, SUCCEEDED },
              { FAILED, ABORTED }
            })
        .forEach(testCase -> {
            assertThatThrownBy(() -> testCase[0].advanceTo(testCase[1]))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasNoCause()
            .hasMessageContaining(commonErrorMsg);
        });
    }

    private void assertAdvanceTo(RestoreRangeStatus from, RestoreRangeStatus to)
    {
        assertThat(from.advanceTo(to)).isEqualTo(to);
    }
}
