package org.apache.cassandra.sidecar.common.utils;

import java.util.Set;

/**
 * An interface to provide validation configuration parameters
 */
public interface ValidationConfiguration
{
    /**
     * @return a set of forbidden keyspaces
     */
    Set<String> getForbiddenKeyspaces();

    /**
     * @return a regular expression for an allowed pattern for directory names
     * (i.e. keyspace directory name or table directory name)
     */
    String getAllowedPatternForDirectory();

    /**
     * @return a regular expression for an allowed pattern for component names
     */
    String getAllowedPatternForComponentName();

    /**
     * @return a regular expression to an allowed pattern for a subset of component names
     */
    String getAllowedPatternForRestrictedComponentName();
}
