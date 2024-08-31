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

package org.apache.cassandra.sidecar.exceptions;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

// Test utility class RestoreJobExceptions
class RestoreJobExceptionsTest
{
    @Test
    void testPropagate()
    {
        assertThat(RestoreJobExceptions.propagate("msg", new RuntimeException()))
        .isInstanceOf(RestoreJobException.class)
        .hasMessage("msg")
        .hasRootCauseInstanceOf(RuntimeException.class);

        assertThat(RestoreJobExceptions.propagate("msg", new RestoreJobFatalException("fatal")))
        .isInstanceOf(RestoreJobFatalException.class)
        .hasMessage("msg:fatal")
        .hasRootCauseInstanceOf(RestoreJobFatalException.class)
        .hasRootCauseMessage("fatal");
    }

    @Test
    void testToFatal()
    {
        assertThat(RestoreJobExceptions.toFatal(new RuntimeException("error")))
        .isInstanceOf(RestoreJobFatalException.class)
        .hasMessage("error")
        .hasRootCauseInstanceOf(RuntimeException.class)
        .hasRootCauseMessage("error");

        RestoreJobFatalException fatalException = new RestoreJobFatalException("fatal");
        assertThat(RestoreJobExceptions.toFatal(fatalException))
        .isSameAs(fatalException);
    }
}
