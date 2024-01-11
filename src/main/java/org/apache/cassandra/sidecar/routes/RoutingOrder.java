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

package org.apache.cassandra.sidecar.routes;

/**
 * Control the routing order when the precise ordering is desired.
 * For example, the auth handler should be evaluated the first, regardless of the declaration order.
 * The route order can be specified via {@linkplain io.vertx.ext.web.Route#order(int)}.
 * Note that routes can be specified with the same order value. In such case, the effective order is
 * determined by the declaration order in the code. See {@code org.apache.cassandra.sidecar.routes.VertxRoutingTest}
 */
public enum RoutingOrder
{
    HIGHEST(Integer.MIN_VALUE),
    HIGH(-9999),
    DEFAULT(0),
    LOW(9999),
    LOWEST(Integer.MAX_VALUE),
    ;

    public final int order;

    RoutingOrder(int order)
    {
        this.order = order;
    }
}
