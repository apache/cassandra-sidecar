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

package org.apache.cassandra.sidecar.common.server.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link ByteUtils}
 */
class ByteUtilsTest
{
    @ParameterizedTest
    @ValueSource(longs = { -1L, -10L, -100L, -1024L, Long.MIN_VALUE })
    void failsWithNegativeInputs(long bytes)
    {
        assertThatIllegalArgumentException().isThrownBy(() -> ByteUtils.bytesToHumanReadableBinaryPrefix(bytes))
                                            .withMessage("bytes cannot be negative");
    }

    @Test
    void testToHumanReadableBinaryPrefix()
    {
        assertThat(ByteUtils.bytesToHumanReadableBinaryPrefix(0)).isEqualTo("0 B");
        assertThat(ByteUtils.bytesToHumanReadableBinaryPrefix(1152921504606846976L)).isEqualTo("1.00 EiB");
        assertThat(ByteUtils.bytesToHumanReadableBinaryPrefix(1152921504606846977L)).isEqualTo("1.00 EiB");
        assertThat(ByteUtils.bytesToHumanReadableBinaryPrefix(Long.MAX_VALUE)).isEqualTo("8.00 EiB");
        assertThat(ByteUtils.bytesToHumanReadableBinaryPrefix(64)).isEqualTo("64 B");
        assertThat(ByteUtils.bytesToHumanReadableBinaryPrefix(3221225472L)).isEqualTo("3.00 GiB");
        assertThat(ByteUtils.bytesToHumanReadableBinaryPrefix(4L * 1024 * 1024)).isEqualTo("4.00 MiB");
        assertThat(ByteUtils.bytesToHumanReadableBinaryPrefix(4L * 1024 * 1024 * 1024)).isEqualTo("4.00 GiB");
        assertThat(ByteUtils.bytesToHumanReadableBinaryPrefix(4L * 1024 * 1024 * 1024 * 1024)).isEqualTo("4.00 TiB");
        assertThat(ByteUtils.bytesToHumanReadableBinaryPrefix(4L * 1024 * 1024 * 1024 * 1024 * 1024))
        .isEqualTo("4.00 PiB");
        assertThat(ByteUtils.bytesToHumanReadableBinaryPrefix(4L * 1024 * 1024 * 1024 * 1024 * 1024 * 1024))
        .isEqualTo("4.00 EiB");
    }
}
