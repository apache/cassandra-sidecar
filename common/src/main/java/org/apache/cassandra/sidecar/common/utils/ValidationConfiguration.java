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
     * @return a patter for allowed characters
     */
    String getCharsAllowedPattern();

    /**
     * @return a regular expression to validate component names
     */
    String getComponentRegex();

    /**
     * @return a regular expression to validate .db and TOC.txt component names
     */
    String getDbTocComponentRegex();
}
