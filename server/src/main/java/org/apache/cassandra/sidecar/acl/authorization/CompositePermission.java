package org.apache.cassandra.sidecar.acl.authorization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.FeatureAuthorization;

import static org.apache.cassandra.sidecar.common.utils.StringUtils.isNotEmpty;

public class CompositePermission extends StandardPermission
{
    private final List<Pair<Permission, String>> permissionsWithResource;

    public CompositePermission(String name, List<Pair<Permission, String>> permissionsWithResource)
    {
        super(name);
        if (permissionsWithResource == null || permissionsWithResource.isEmpty())
        {
            throw new IllegalArgumentException("CompositePermission can not be created with null or empty permissions");
        }
        this.permissionsWithResource = Collections.unmodifiableList(permissionsWithResource);
    }

    @Override
    public Authorization toAuthorization(String resource)
    {
        // create childAuthorizations in every toAuthorization call, Authorization objects overrides existing
        // resource during setResource call. Pre creating Authorization object could lead to resource getting overridden
        // when a user holds same permission across different resources
        List<Authorization> authorizations = new ArrayList<>();
        for (Pair<Permission, String> permissionResourcePair : permissionsWithResource)
        {
            Permission composedPermission = permissionResourcePair.getKey();
            String composedResource = permissionResourcePair.getRight();
            authorizations.add(composedPermission.toAuthorization(composedResource));
        }
        FeatureAuthorization authorization = FeatureAuthorization.create(authorizations);
        if (isNotEmpty(resource))
        {
            authorization.setResource(resource);
        }
        return authorization;
    }
}
