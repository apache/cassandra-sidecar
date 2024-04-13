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

package org.apache.cassandra.sidecar.config.yaml;


import org.apache.cassandra.sidecar.config.ServiceConfiguration;

/**
 * A ServiceConfiguration implementation created for test.
 * It binds to "127.0.0.1" and port 0 to find an available port at test runtime
 */
public class TestServiceConfiguration extends ServiceConfigurationImpl
{
    public static ServiceConfiguration newInstance()
    {
        return builder().build();
    }

    public static Builder builder()
    {
        return ServiceConfigurationImpl.builder()
                                       .host("127.0.0.1")
                                       .port(0); // let the test find an available port
    }
}
