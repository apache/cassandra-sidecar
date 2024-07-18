/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common.request;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.data.RestoreJobProgressFetchPolicy;
import org.apache.cassandra.sidecar.common.request.data.RestoreJobProgressRequestParams;

import static org.assertj.core.api.Assertions.assertThat;

class RestoreJobProgressRequestTest
{
    @Test
    void testBuildRequestURI()
    {
        UUID jobId = UUID.randomUUID();
        RestoreJobProgressRequestParams params = new RestoreJobProgressRequestParams("cycling",
                                                                                     "rank_by_year_and_name",
                                                                                     jobId,
                                                                                     RestoreJobProgressFetchPolicy.FIRST_FAILED);
        String uri = RestoreJobProgressRequest.requestURI(params);
        assertThat(uri).isEqualTo("/api/v1/keyspaces/cycling/tables/rank_by_year_and_name" +
                                  "/restore-jobs/" + jobId +
                                  "/progress?fetch-policy=first_failed");
    }
}
