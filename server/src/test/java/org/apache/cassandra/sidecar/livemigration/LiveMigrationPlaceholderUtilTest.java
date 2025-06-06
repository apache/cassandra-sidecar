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

package org.apache.cassandra.sidecar.livemigration;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.hasAnyPlaceholder;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.replacePlaceholder;
import static org.assertj.core.api.Assertions.assertThat;

class LiveMigrationPlaceholderUtilTest
{
    @Test
    public void testReplacePlaceholder()
    {
        assertThat(replacePlaceholder("/no/placeholder/dir", Collections.singleton("DATA_DIR"), "data_dir"))
        .isEqualTo("/no/placeholder/dir");

        assertThat(replacePlaceholder("${DATA_DIR}/ks1", Collections.singleton("DATA_DIR"), "/data_dir"))
        .isEqualTo("/data_dir/ks1");

        // replacement of placeholder happens if and only if the placeholder given is found.
        assertThat(replacePlaceholder("${HINTS_DIR}/ks1", Collections.singleton("DATA_DIR"), "/data_dir"))
        .isNull();
    }

    @Test
    void testHasAnyPlaceHolder()
    {
        assertThat(hasAnyPlaceholder("glob:/var/log/cassandra/data")).isFalse();
        assertThat(hasAnyPlaceholder("/var/log/cassandra/data")).isFalse();
        assertThat(hasAnyPlaceholder("glob:/var/log/cassandra/data/**")).isFalse();
        assertThat(hasAnyPlaceholder("glob:${SOME_DIR}/cassandra/data/**")).isTrue();
        assertThat(hasAnyPlaceholder("regex:${SOME_DIR_1}/cassandra/data/**")).isTrue();
        assertThat(hasAnyPlaceholder("${SOME_DIR_2}/cassandra/data/**")).isTrue();
        assertThat(hasAnyPlaceholder("{SOME_DIR_2}/cassandra/data/**")).isFalse();
    }

    @Test
    void testHasKnownPlaceHolder()
    {
        assertThat(hasAnyPlaceholder("${DATA_FILE_DIR}/ks/t1", Collections.singleton("HINTS_DIR"))).isFalse();
        assertThat(hasAnyPlaceholder("glob:${DATA_FILE_DIR}/ks/t1", Collections.singleton("HINTS_DIR"))).isFalse();


        assertThat(hasAnyPlaceholder("${HINTS_DIR}/ks/t1", Collections.singleton("HINTS_DIR"))).isTrue();
        assertThat(hasAnyPlaceholder("glob:${HINTS_DIR}/ks/t1", Collections.singleton("HINTS_DIR"))).isTrue();

        assertThat(hasAnyPlaceholder("${DATA_FILE_DIR}/ks/t1", Collections.singleton("DATA_FILE_DIR"))).isTrue();
        assertThat(hasAnyPlaceholder("glob:${DATA_FILE_DIR}/ks/t1", Collections.singleton("DATA_FILE_DIR"))).isTrue();
        assertThat(hasAnyPlaceholder("regex:${DATA_FILE_DIR}/k*/t1", Collections.singleton("DATA_FILE_DIR"))).isTrue();

        assertThat(hasAnyPlaceholder("glob:${DATA_FILE_DIR}/ks/t1", Collections.singleton("DATA_FILE_DIR_0"))).isFalse();
    }
}
