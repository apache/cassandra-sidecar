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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.sidecar.common.utils.ValidationConfiguration;

/**
 * A {@link ValidationConfiguration} used for unit testing
 */
public class TestValidationConfiguration implements ValidationConfiguration
{
    private static final Set<String> FORBIDDEN_KEYSPACES = new HashSet<>(Arrays.asList("system_schema",
                                                                                         "system_traces",
                                                                                         "system_distributed",
                                                                                         "system",
                                                                                         "system_auth",
                                                                                         "system_views",
                                                                                         "system_virtual_schema"));
    private static final String ALLOWED_PATTERN_FOR_DIRECTORY = "[a-zA-Z0-9_-]+";
    private static final String ALLOWED_PATTERN_FOR_COMPONENT_NAME = ALLOWED_PATTERN_FOR_DIRECTORY
                                                                       + "(.db|.cql|.json|.crc32|TOC.txt)";
    private static final String ALLOWED_PATTERN_FOR_RESTRICTED_COMPONENT_NAME = ALLOWED_PATTERN_FOR_DIRECTORY
                                                                                  + "(.db|TOC.txt)";

    public Set<String> forbiddenKeyspaces()
    {
        return FORBIDDEN_KEYSPACES;
    }

    public String allowedPatternForDirectory()
    {
        return ALLOWED_PATTERN_FOR_DIRECTORY;
    }

    public String allowedPatternForComponentName()
    {
        return ALLOWED_PATTERN_FOR_COMPONENT_NAME;
    }

    public String allowedPatternForRestrictedComponentName()
    {
        return ALLOWED_PATTERN_FOR_RESTRICTED_COMPONENT_NAME;
    }
}
