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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.CassandraInputValidationConfiguration;

/**
 * Encapsulate configuration values for validation properties used for Cassandra inputs
 */
@Binds(to = CassandraInputValidationConfiguration.class)
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
                                                            "system_virtual_schema",
                                                            "sidecar_internal")));
    public static final String ALLOWED_CHARS_FOR_NAME_PROPERTY = "allowed_chars_for_directory";
    public static final String DEFAULT_ALLOWED_CHARS_FOR_NAME = "[a-zA-Z][a-zA-Z0-9_]{0,47}";
    public static final String ALLOWED_CHARS_FOR_QUOTED_NAME_PROPERTY = "allowed_chars_for_quoted_name";
    public static final String DEFAULT_ALLOWED_CHARS_FOR_QUOTED_NAME = "[a-zA-Z_0-9]{1,48}";
    public static final String ALLOWED_CHARS_FOR_COMPONENT_NAME_PROPERTY = "allowed_chars_for_component_name";
    public static final String DEFAULT_ALLOWED_CHARS_FOR_COMPONENT_NAME =
    "[a-zA-Z0-9_-]+(.db|.cql|.json|.crc32|TOC.txt)";
    public static final String ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME_PROPERTY =
    "allowed_chars_for_restricted_component_name";
    public static final String DEFAULT_ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME = "[a-zA-Z0-9_-]+(.db|TOC.txt)";

    @JsonProperty(FORBIDDEN_KEYSPACES_PROPERTY)
    protected final Set<String> forbiddenKeyspaces;

    @JsonProperty(value = ALLOWED_CHARS_FOR_NAME_PROPERTY, defaultValue = DEFAULT_ALLOWED_CHARS_FOR_NAME)
    protected final String allowedPatternForName;

    @JsonProperty(value = ALLOWED_CHARS_FOR_QUOTED_NAME_PROPERTY, defaultValue = DEFAULT_ALLOWED_CHARS_FOR_QUOTED_NAME)
    protected final String allowedPatternForQuotedName;

    @JsonProperty(value = ALLOWED_CHARS_FOR_COMPONENT_NAME_PROPERTY,
    defaultValue = DEFAULT_ALLOWED_CHARS_FOR_COMPONENT_NAME)
    protected final String allowedPatternForComponentName;

    @JsonProperty(value = ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME_PROPERTY,
    defaultValue = DEFAULT_ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME)
    protected final String allowedPatternForRestrictedComponentName;

    public CassandraInputValidationConfigurationImpl()
    {
        this(DEFAULT_FORBIDDEN_KEYSPACES,
             DEFAULT_ALLOWED_CHARS_FOR_NAME,
             DEFAULT_ALLOWED_CHARS_FOR_QUOTED_NAME,
             DEFAULT_ALLOWED_CHARS_FOR_COMPONENT_NAME,
             DEFAULT_ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME);
    }

    public CassandraInputValidationConfigurationImpl(Set<String> forbiddenKeyspaces,
                                                     String allowedPatternForName,
                                                     String allowedPatternForQuotedName,
                                                     String allowedPatternForComponentName,
                                                     String allowedPatternForRestrictedComponentName)
    {
        this.forbiddenKeyspaces = forbiddenKeyspaces;
        this.allowedPatternForName = allowedPatternForName;
        this.allowedPatternForQuotedName = allowedPatternForQuotedName;
        this.allowedPatternForComponentName = allowedPatternForComponentName;
        this.allowedPatternForRestrictedComponentName = allowedPatternForRestrictedComponentName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(FORBIDDEN_KEYSPACES_PROPERTY)
    public Set<String> forbiddenKeyspaces()
    {
        return forbiddenKeyspaces;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = ALLOWED_CHARS_FOR_NAME_PROPERTY, defaultValue = DEFAULT_ALLOWED_CHARS_FOR_NAME)
    public String allowedPatternForName()
    {
        return allowedPatternForName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = ALLOWED_CHARS_FOR_QUOTED_NAME_PROPERTY, defaultValue = DEFAULT_ALLOWED_CHARS_FOR_QUOTED_NAME)
    public String allowedPatternForQuotedName()
    {
        return allowedPatternForQuotedName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = ALLOWED_CHARS_FOR_COMPONENT_NAME_PROPERTY,
    defaultValue = DEFAULT_ALLOWED_CHARS_FOR_COMPONENT_NAME)
    public String allowedPatternForComponentName()
    {
        return allowedPatternForComponentName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty(value = ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME_PROPERTY,
    defaultValue = DEFAULT_ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME)
    public String allowedPatternForRestrictedComponentName()
    {
        return allowedPatternForRestrictedComponentName;
    }
}
