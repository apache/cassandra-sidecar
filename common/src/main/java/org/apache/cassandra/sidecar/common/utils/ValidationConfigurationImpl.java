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

    public String getComponentRegex()
    {
        return REGEX_COMPONENT;
    }

    public String getDbTocComponentRegex()
    {
        return REGEX_DB_TOC_COMPONENT;
    }
}
