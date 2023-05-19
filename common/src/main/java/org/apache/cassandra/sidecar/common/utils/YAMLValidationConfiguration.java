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
 * An implementation that reads the {@link ValidationConfiguration} from a {@code YAMLConfiguration}.
 */
public class YAMLValidationConfiguration implements ValidationConfiguration
{
    private final Set<String> forbiddenKeyspaces;
    private final String allowedPatternForDirectory;
    private final String allowedPatternForComponentName;
    private final String allowedPatternForRestrictedComponentName;

    public YAMLValidationConfiguration(Set<String> forbiddenKeyspaces,
                                       String allowedPatternForDirectory,
                                       String allowedPatternForComponentName,
                                       String allowedPatternForRestrictedComponentName)
    {
        this.forbiddenKeyspaces = forbiddenKeyspaces;
        this.allowedPatternForDirectory = allowedPatternForDirectory;
        this.allowedPatternForComponentName = allowedPatternForComponentName;
        this.allowedPatternForRestrictedComponentName = allowedPatternForRestrictedComponentName;
    }

    public Set<String> forbiddenKeyspaces()
    {
        return forbiddenKeyspaces;
    }

    public String allowedPatternForDirectory()
    {
        return allowedPatternForDirectory;
    }

    public String allowedPatternForComponentName()
    {
        return allowedPatternForComponentName;
    }

    public String allowedPatternForRestrictedComponentName()
    {
        return allowedPatternForRestrictedComponentName;
    }
}
