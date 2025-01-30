package org.apache.cassandra.sidecar.acl.authorization;

import java.util.Set;

/**
 * Resource type sidecar can expect permissions for.
 */
public interface ResourceScope
{
    /**
     * @return variable aware resource built with this scope, to be used in Handlers for building required
     * authorizations
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
