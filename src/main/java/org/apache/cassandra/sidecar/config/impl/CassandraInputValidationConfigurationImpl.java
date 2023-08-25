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

package org.apache.cassandra.sidecar.config.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.CassandraInputValidationConfiguration;

/**
 * Encapsulate configuration values for validation properties used for Cassandra inputs
 */
public class CassandraInputValidationConfigurationImpl implements CassandraInputValidationConfiguration
{
    public static final String FORBIDDEN_KEYSPACES_PROPERTY = "forbidden_keyspaces";
    public static final Set<String> DEFAULT_FORBIDDEN_KEYSPACES =
    Collections.unmodifiableSet(new HashSet<>(Arrays.asList("system_schema",
                                                            "system_traces",
                                                            "system_distributed",
                                                            "system",
                                                            "system_auth",
                                                            "system_views",
                                                            "system_virtual_schema")));
    public static final String ALLOWED_CHARS_FOR_DIRECTORY_PROPERTY = "allowed_chars_for_directory";
    public static final String DEFAULT_ALLOWED_CHARS_FOR_DIRECTORY = "[a-zA-Z0-9_-]+";
    public static final String ALLOWED_CHARS_FOR_COMPONENT_NAME_PROPERTY = "allowed_chars_for_component_name";
    public static final String DEFAULT_ALLOWED_CHARS_FOR_COMPONENT_NAME =
    "[a-zA-Z0-9_-]+(.db|.cql|.json|.crc32|TOC.txt)";
    public static final String ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME_PROPERTY =
    "allowed_chars_for_restricted_component_name";
    public static final String DEFAULT_ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME = "[a-zA-Z0-9_-]+(.db|TOC.txt)";

    @JsonProperty(FORBIDDEN_KEYSPACES_PROPERTY)
    protected final Set<String> forbiddenKeyspaces;

    @JsonProperty(value = ALLOWED_CHARS_FOR_DIRECTORY_PROPERTY, defaultValue = DEFAULT_ALLOWED_CHARS_FOR_DIRECTORY)
    protected final String allowedPatternForDirectory;

    @JsonProperty(value = ALLOWED_CHARS_FOR_COMPONENT_NAME_PROPERTY,
    defaultValue = DEFAULT_ALLOWED_CHARS_FOR_COMPONENT_NAME)
    protected final String allowedPatternForComponentName;

    @JsonProperty(value = ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME_PROPERTY,
    defaultValue = DEFAULT_ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME)
    protected final String allowedPatternForRestrictedComponentName;

    public CassandraInputValidationConfigurationImpl()
    {
        this(DEFAULT_FORBIDDEN_KEYSPACES,
             DEFAULT_ALLOWED_CHARS_FOR_DIRECTORY,
             DEFAULT_ALLOWED_CHARS_FOR_COMPONENT_NAME,
             DEFAULT_ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME);
    }

    public CassandraInputValidationConfigurationImpl(Set<String> forbiddenKeyspaces,
                                                     String allowedPatternForDirectory,
                                                     String allowedPatternForComponentName,
                                                     String allowedPatternForRestrictedComponentName)
    {
        this.forbiddenKeyspaces = forbiddenKeyspaces;
        this.allowedPatternForDirectory = allowedPatternForDirectory;
        this.allowedPatternForComponentName = allowedPatternForComponentName;
        this.allowedPatternForRestrictedComponentName = allowedPatternForRestrictedComponentName;
    }

    /**
     * @return a set of forbidden keyspaces
     */
    @Override
    @JsonProperty(FORBIDDEN_KEYSPACES_PROPERTY)
    public Set<String> forbiddenKeyspaces()
    {
        return forbiddenKeyspaces;
    }

    /**
     * @return a regular expression for an allowed pattern for directory names
     * (i.e. keyspace directory name or table directory name)
     */
    @Override
    @JsonProperty(value = ALLOWED_CHARS_FOR_DIRECTORY_PROPERTY, defaultValue = DEFAULT_ALLOWED_CHARS_FOR_DIRECTORY)
    public String allowedPatternForDirectory()
    {
        return allowedPatternForDirectory;
    }

    /**
     * @return a regular expression for an allowed pattern for component names
     */
    @Override
    @JsonProperty(value = ALLOWED_CHARS_FOR_COMPONENT_NAME_PROPERTY,
    defaultValue = DEFAULT_ALLOWED_CHARS_FOR_COMPONENT_NAME)
    public String allowedPatternForComponentName()
    {
        return allowedPatternForComponentName;
    }

    /**
     * @return a regular expression to an allowed pattern for a subset of component names
     */
    @Override
    @JsonProperty(value = ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME_PROPERTY,
    defaultValue = DEFAULT_ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME)
    public String allowedPatternForRestrictedComponentName()
    {
        return allowedPatternForRestrictedComponentName;
    }
}
