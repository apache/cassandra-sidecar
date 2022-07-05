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

package org.apache.cassandra.sidecar.common.utils;

import java.util.Set;

/**
 * An interface to provide validation configuration parameters
 */
public interface ValidationConfiguration
{
    /**
     * @return a set of forbidden directories
     */
    Set<String> getForbiddenDirs();

    /**
     * @return a pattern for allowed characters
     */
    String getCharsAllowedPattern();

    /**
     * @return a regular expression to validate component names
     */
    String getCharsAllowedPatternForComponentName();

    /**
     * @return a regular expression to validate a restricted set of component names
     */
    String getCharsAllowedPatternForRestrictedComponentName();
}
