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

    public Set<String> getForbiddenKeyspaces()
    {
        return FORBIDDEN_KEYSPACES;
    }

    public String getAllowedPatternForDirectory()
    {
        return ALLOWED_PATTERN_FOR_DIRECTORY;
    }

    public String getAllowedPatternForComponentName()
    {
        return ALLOWED_PATTERN_FOR_COMPONENT_NAME;
    }

    public String getAllowedPatternForRestrictedComponentName()
    {
        return ALLOWED_PATTERN_FOR_RESTRICTED_COMPONENT_NAME;
    }
}
