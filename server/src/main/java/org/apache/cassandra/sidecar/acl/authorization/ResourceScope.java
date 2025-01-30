package org.apache.cassandra.sidecar.acl.authorization;

import java.util.Set;

/**
 * Resource scope that can be set in permissions.
 */
public interface ResourceScope
{
    /**
     * @return variable aware resource built with this scope, to be used in Handlers for building required
     * authorizations. Variables in a resource can be denoted by enclosing them in curly braces. For e.g. for data
     * resource scoped at keyspace level, variableAwareResource returned would be data/{keyspace}. These variables are
     * populated when a request is received using request parameters and authorizations are extracted for resolved
     * resource.
     */
    String variableAwareResource();

    /**
     * Given a resource builds resolved resource for this scope.
     *
     * @param resource resource set
     * @return resolved resource built with set scope and given resource. Mainly used for feature level
     * permissions to build resource for child permissions with child permission's scope and parent
     * permission's resource.
     */
    String resolveWithResource(String resource);

    /**
     * @return {@code Set} of expanded resources this resource scope can accept authorization for.
     */
    Set<String> expandedResources();
}
