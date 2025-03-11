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

package org.apache.cassandra.sidecar.modules.multibindings;

import org.junit.jupiter.api.Test;

import io.vertx.core.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys.AbortRestoreJobRouteKey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RouteClassKeyTest
{
    @Test
    void testExtractStaticFields()
    {
        assertThat(RouteClassKey.httpMethod(AbortRestoreJobRouteKey.class)).isEqualTo(HttpMethod.POST);
        assertThat(RouteClassKey.routeURI(AbortRestoreJobRouteKey.class)).isEqualTo(ApiEndpointsV1.ABORT_RESTORE_JOB_ROUTE);
    }

    @Test
    void testExtractAbsentStaticFieldShouldFail()
    {
        assertThatThrownBy(() -> RouteClassKey.httpMethod(EmptyKey.class))
        .isExactlyInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(NoSuchFieldException.class)
        .hasMessage("java.lang.NoSuchFieldException: HTTP_METHOD");

        assertThatThrownBy(() -> RouteClassKey.routeURI(EmptyKey.class))
        .isExactlyInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(NoSuchFieldException.class)
        .hasMessage("java.lang.NoSuchFieldException: ROUTE_URI");
    }

    @Test
    void testExtractFromMalformedKey()
    {
        assertThatThrownBy(() -> RouteClassKey.httpMethod(MalformedKey.class))
        .isExactlyInstanceOf(RuntimeException.class)
        .hasCauseInstanceOf(ClassCastException.class)
        .hasMessageContaining("Cannot cast java.lang.String to io.vertx.core.http.HttpMethod");
    }

    interface EmptyKey extends RouteClassKey {}
    interface MalformedKey extends RouteClassKey
    {
        String HTTP_METHOD = "POST";
    }
}
