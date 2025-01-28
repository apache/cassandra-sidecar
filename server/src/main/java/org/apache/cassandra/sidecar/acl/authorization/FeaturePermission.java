/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.acl.authorization;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import io.vertx.ext.auth.authorization.FeatureAuthorization;

import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.CREATE_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.CREATE_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.EDIT_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.IMPORT_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_RESTORE_JOB;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_RING;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_SCHEMA;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_TOPOLOGY;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.STREAM_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.UPLOAD_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.CassandraPermissions.SELECT;
import static org.apache.cassandra.sidecar.acl.authorization.VariableAwareResource.CLUSTER;
import static org.apache.cassandra.sidecar.acl.authorization.VariableAwareResource.DATA;
import static org.apache.cassandra.sidecar.acl.authorization.VariableAwareResource.DATA_WITH_KEYSPACE;
import static org.apache.cassandra.sidecar.acl.authorization.VariableAwareResource.DATA_WITH_KEYSPACE_TABLE;

/**
 * Enumerates a list of feature level permissions that Sidecar recognizes and honors. Feature level permissions
 * uses {@link FeatureAuthorization} to represent authorizations granted/required. {@link FeatureAuthorization}
 * could be composed of multiple authorizations.
 */
public enum FeaturePermission
{
    BULK_READ_DIRECT("BULK_READ_DIRECT",
                     Pair.of(READ_RING, DATA.resource()),
                     Pair.of(READ_RING, DATA_WITH_KEYSPACE.resource()),
                     Pair.of(READ_SCHEMA, DATA.resource()),
                     Pair.of(READ_SCHEMA, DATA_WITH_KEYSPACE.resource()),
                     Pair.of(CREATE_SNAPSHOT, DATA_WITH_KEYSPACE_TABLE.resource()),
                     Pair.of(READ_SNAPSHOT, DATA_WITH_KEYSPACE_TABLE.resource()),
                     Pair.of(DELETE_SNAPSHOT, DATA_WITH_KEYSPACE_TABLE.resource()),
                     Pair.of(STREAM_SNAPSHOT, DATA_WITH_KEYSPACE_TABLE.resource()),
                     Pair.of(SELECT, DATA_WITH_KEYSPACE_TABLE.resource())),

    BULK_WRITE_DIRECT("BULK_WRITE_DIRECT",
                      Pair.of(READ_SCHEMA, DATA_WITH_KEYSPACE.resource()),
                      Pair.of(READ_TOPOLOGY, DATA_WITH_KEYSPACE.resource()),
                      Pair.of(UPLOAD_STAGED_SSTABLE, DATA_WITH_KEYSPACE_TABLE.resource()),
                      Pair.of(IMPORT_STAGED_SSTABLE, DATA_WITH_KEYSPACE_TABLE.resource()),
                      Pair.of(DELETE_STAGED_SSTABLE, DATA_WITH_KEYSPACE_TABLE.resource())),

    BULK_WRITE_S3_COMPAT("BULK_WRITE_S3_COMPAT",
                         Pair.of(READ_SCHEMA, DATA_WITH_KEYSPACE.resource()),
                         Pair.of(READ_TOPOLOGY, DATA_WITH_KEYSPACE.resource()),
                         Pair.of(CREATE_RESTORE_JOB, DATA_WITH_KEYSPACE_TABLE.resource()),
                         Pair.of(READ_RESTORE_JOB, DATA_WITH_KEYSPACE_TABLE.resource()),
                         Pair.of(EDIT_RESTORE_JOB, DATA_WITH_KEYSPACE_TABLE.resource()),
                         Pair.of(DELETE_RESTORE_JOB, DATA_WITH_KEYSPACE_TABLE.resource())),

    CDC("CDC", Pair.of(BasicPermissions.CDC, CLUSTER.resource())),
    ;

    private static final Map<String, FeaturePermission> NAME_TO_FEATURE_PERMISSION
    = Arrays.stream(values())
            .collect(Collectors.collectingAndThen(Collectors.toMap(Enum::name, Function.identity()), Collections::unmodifiableMap));

    private final CompositePermission permission;

    FeaturePermission(String name, Pair<Permission, String>... permissionResourcePair)
    {
        this.permission
        = new CompositePermission(name, Arrays.stream(permissionResourcePair).collect(Collectors.toUnmodifiableList()));
    }

    public Permission permission()
    {
        return permission;
    }

    public static boolean contains(String name)
    {
        return NAME_TO_FEATURE_PERMISSION.containsKey(name);
    }

    public static Permission fromName(String name)
    {
        FeaturePermission featurePermission = NAME_TO_FEATURE_PERMISSION.get(name);
        return featurePermission != null ? featurePermission.permission() : null;
    }
}
