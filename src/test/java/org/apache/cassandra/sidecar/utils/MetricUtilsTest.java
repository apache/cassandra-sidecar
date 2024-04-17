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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test utility methods provided for metrics related operations.
 */
class MetricUtilsTest
{
    @ParameterizedTest
    @CsvSource({"nb-1-big-Data.db,Data.db", "nb-1-big-CompressionInfo.db   ,CompressionInfo.db",
                "nb-1-big-Digest.crc32,Digest.crc32", "     nb-1-big-Filter.db,Filter.db", "nb-1-big-Index.db,Index.db",
                "nb-1-big-Statistics.db,Statistics.db", "nb-1-big-Summary.db,Summary.db", "nb-1-big-UnexpectedFormat.db,unknown",
                "nb-1-big-TOC.txt,unknown"})
    void parseDataComponent(String filename, String expectedComponent)
    {
        assertThat(MetricUtils.parseSSTableComponent(filename)).isEqualTo(expectedComponent);
    }
}
