package org.apache.cassandra.sidecar.acl.authorization;

import java.util.Collections;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.KEYSPACE;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.TABLE;
import static org.apache.cassandra.sidecar.common.utils.StringUtils.isNullOrEmpty;

/**
 * Signifies scope of data resource.
 *
 * Cassandra stores data resource in the format data, data/keyspace or data/keyspace_name/table_name within
 * the role_permissions table. A similar format is followed for storing data resources in sidecar permissions
 * table role_permissions_v1. Hence, sidecar endpoints expect data resources to be provided in format
 * data/keyspace_name/table_name.
 * <p>
 * In this context, curly braces are used to denote variable parts of the resource. For e.g., when permissions are
 * checked for resource data/{keyspace} in an endpoint, the part within the curly braces ({keyspace})
 * represents a placeholder for the actual keyspace name provided as a path parameter. For more context refer to
 * io.vertx.ext.auth.authorization.impl.VariableAwareExpression
 * <p>
 * During the permission matching process, the placeholder {keyspace} is resolved to the actual keyspace
 * being accessed by the endpoint. For e.g. data/{keyspace} resolves to data/university if the keyspace is
 * "university".
 * <p>
 * User permissions are then extracted from both Cassandra and sidecar role permissions tables for
 * the resolved resource and are matched against the expected permissions set defined in the endpoint's handler.
 */
public class DataResourceScope implements ResourceScope
{
    private static final String DATA = "data";

    private static final String DATA_WITH_KEYSPACE = String.format("data/{%s}", KEYSPACE);

    // TODO remove this hack once VariableAwareExpression bug is fixed
    // VariableAwareExpression in vertx-auth-common package has a bug during String.substring() call, hence
    // we cannot set resources that do not end in curly braces (e.g. data/keyspace/*) in
    // PermissionBasedAuthorizationImpl or WildcardPermissionBasedAuthorizationImpl. data/{%s}/{TABLE_WILDCARD} treats
    // TABLE_WILDCARD as a variable. This hack allows to read resource level permissions that could be set for all
    // tables through data/<keyspace_name>/*. Bug should be fixed in 4.5.12
    // Note: DATA_WITH_KEYSPACE_ALL_TABLES resource comprises all tables under the keyspace excluding the keyspace itself
    private static final String DATA_WITH_KEYSPACE_ALL_TABLES = String.format("data/{%s}/{TABLE_WILDCARD}", KEYSPACE);

    private static final String DATA_WITH_KEYSPACE_TABLE = String.format("data/{%s}/{%s}", KEYSPACE, TABLE);

    private final boolean expectKeyspace;
    private final boolean expectTable;
    private final Set<String> expandedResources;

    public DataResourceScope()
    {
        this(false, false);
    }

    public DataResourceScope(boolean expectKeyspace)
    {
        this(expectKeyspace, false);
    }

    public DataResourceScope(boolean expectKeyspace, boolean expectTable)
    {
        this.expectKeyspace = expectKeyspace;
        this.expectTable = expectTable;
        this.expandedResources = initializeExpandedResources();
    }

    private Set<String> initializeExpandedResources()
    {
        if (expectTable)
        {
            // can expand to DATA, DATA_WITH_KEYSPACE and DATA_WITH_KEYSPACE_ALL_TABLES
            return ImmutableSet.of(DATA, DATA_WITH_KEYSPACE, DATA_WITH_KEYSPACE_ALL_TABLES, DATA_WITH_KEYSPACE_TABLE);
        }
        if (expectKeyspace)
        {
            // can expand to DATA
            return ImmutableSet.of(DATA, DATA_WITH_KEYSPACE);
        }
        return Collections.singleton(DATA);
    }

    @Override
    public String variableAwareResource()
    {
        if (expectTable)
        {
            return DATA_WITH_KEYSPACE_TABLE;
        }
        if (expectKeyspace)
        {
            return DATA_WITH_KEYSPACE;
        }
        return DATA;
    }

    @Override
    public String resolveWithResource(String resource)
    {
        if (isNullOrEmpty(resource) || !resource.startsWith("data"))
        {
            return variableAwareResource();
        }

        String[] parts = resource.split("/");
        if (expectTable)
        {
            return resource;
        }
        else if (expectKeyspace)
        {
            return parts.length == 3 ? "data/" + parts[1] : resource;
        }
        return "data";
    }

    @Override
    public Set<String> expandedResources()
    {
        return expandedResources;
    }
}
