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
package org.apache.cassandra.sidecar.common;

/**
 * A constants container class for API endpoints of version 2.
 */
public class ApiEndpointsV2
{
    public static final String API = "/api";
    public static final String API_V2 = API + "/v2";
    public static final String CASSANDRA = "/cassandra";
    public static final String NODE_SETTINGS_ROUTE = API_V2 + CASSANDRA + "/settings";
}
