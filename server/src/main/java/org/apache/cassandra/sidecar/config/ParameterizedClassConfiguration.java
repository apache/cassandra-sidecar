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

package org.apache.cassandra.sidecar.config;

import java.util.Map;

import org.jetbrains.annotations.Nullable;

/**
 * Encapsulates configuration needed for classes that can be initialized with a given class name using the
 * configured optional parameters
 */
public interface ParameterizedClassConfiguration
{
    /**
     * @return class name of class to be created with given configuration
     */
    String className();

    /**
     * @return parameters that will be used for creating an object of given class name
     */
    @Nullable
    Map<String, String> namedParameters();
}
