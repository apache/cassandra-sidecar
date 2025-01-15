package org.apache.cassandra.sidecar.acl.authorization;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;

import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.CREATE_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.CREATE_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.EDIT_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.IMPORT_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_CDC;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_NODE_SETTINGS;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_RING;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_SCHEMA;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_TIME_SKEW;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_TOPOLOGY;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.STREAM_CDC;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.STREAM_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.UPLOAD_STAGED_SSTABLE;

/**
 * Enumerates a list of feature level permissions that Sidecar recognizes and honors. Feature level permissions
 * are {@link CompositePermission} that could be composed of basic permissions or other composite permissions.
 */
public enum FeaturePermission
{
    BULK_READ_DIRECT("BULK_READ:DIRECT",
                     READ_NODE_SETTINGS, READ_RING, READ_SCHEMA, CREATE_SNAPSHOT, READ_SNAPSHOT, DELETE_SNAPSHOT,
                     STREAM_SNAPSHOT, CassandraPermissions.SELECT),

    BULK_WRITE_DIRECT("BULK_WRITE:DIRECT",
                      READ_NODE_SETTINGS, READ_TIME_SKEW, READ_SCHEMA, READ_TOPOLOGY, UPLOAD_STAGED_SSTABLE,
                      IMPORT_STAGED_SSTABLE, DELETE_STAGED_SSTABLE),

    BULK_WRITE_S3_COMPAT("BULK_WRITE:S3_COMPAT",
                         Arrays.asList(BULK_WRITE_DIRECT.permission.permissions(), CREATE_RESTORE_JOB, READ_RESTORE_JOB,
                                       EDIT_RESTORE_JOB, DELETE_RESTORE_JOB).toArray(new Permission[0])),

    CDC("CDC", READ_CDC, STREAM_CDC);

    private final CompositePermission permission;

    FeaturePermission(String name, Permission... permissions)
    {
        Set<Permission> childPermissions = Arrays.stream(permissions).collect(Collectors.toSet());
        permission = new CompositePermission(name, childPermissions);
    }

    public Permission permission()
    {
        return permission;
    }

    private static final Map<String, FeaturePermission> nameToFeaturePermission
    = ImmutableMap.of(BULK_READ_DIRECT.name(), BULK_READ_DIRECT, BULK_WRITE_DIRECT.name(), BULK_WRITE_DIRECT,
                      BULK_WRITE_S3_COMPAT.name(), BULK_WRITE_S3_COMPAT, CDC.name(), CDC);

    public static boolean contains(String name)
    {
        return nameToFeaturePermission.containsKey(name);
    }

    public static Permission fromName(String name)
    {
        if (nameToFeaturePermission.containsKey(name))
        {
            return nameToFeaturePermission.get(name).permission;
        }
        return null;
    }
}
