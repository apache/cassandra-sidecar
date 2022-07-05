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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.inject.Singleton;

/**
 * An implementation that provides a default {@link ValidationConfiguration}
 */
@Singleton
public class ValidationConfigurationImpl implements ValidationConfiguration
{
    protected static final Set<String> FORBIDDEN_DIRS = new HashSet<>(Arrays.asList("system_schema",
                                                                                    "system_traces",
                                                                                    "system_distributed",
                                                                                    "system",
                                                                                    "system_auth",
                                                                                    "system_views",
                                                                                    "system_virtual_schema"));
    protected static final String CHARS_ALLOWED_PATTERN = "[a-zA-Z0-9_-]+";
    protected static final String REGEX_COMPONENT = CHARS_ALLOWED_PATTERN + "(.db|.cql|.json|.crc32|TOC.txt)";
    protected static final String REGEX_DB_TOC_COMPONENT = CHARS_ALLOWED_PATTERN + "(.db|TOC.txt)";

    public Set<String> getForbiddenDirs()
    {
        return FORBIDDEN_DIRS;
    }

    public String getCharsAllowedPattern()
    {
        return CHARS_ALLOWED_PATTERN;
    }

    public String getCharsAllowedPatternForComponentName()
    {
        return REGEX_COMPONENT;
    }

    public String getCharsAllowedPatternForRestrictedComponentName()
    {
        return REGEX_DB_TOC_COMPONENT;
    }
}
