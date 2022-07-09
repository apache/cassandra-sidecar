package org.apache.cassandra.sidecar.common.utils;

import java.util.Set;

import org.apache.commons.configuration2.YAMLConfiguration;

import com.google.inject.Singleton;

/**
 * An implementation that reads the {@link ValidationConfiguration} from a {@link YAMLConfiguration}.
 */
@Singleton
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

    public Set<String> getForbiddenKeyspaces()
    {
        return forbiddenKeyspaces;
    }

    public String getAllowedPatternForDirectory()
    {
        return allowedPatternForDirectory;
    }

    public String getAllowedPatternForComponentName()
    {
        return allowedPatternForComponentName;
    }

    public String getAllowedPatternForRestrictedComponentName()
    {
        return allowedPatternForRestrictedComponentName;
    }
}
