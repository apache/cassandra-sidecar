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

import java.util.Collections;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.vertx.ext.auth.authorization.Authorization;

import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.CREATE_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.DELETE_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.IMPORT_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_RING;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_RING_KEYSPACE_SCOPED;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_SCHEMA;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_SCHEMA_KEYSPACE_SCOPED;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.READ_TOPOLOGY;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.STREAM_SNAPSHOT;
import static org.apache.cassandra.sidecar.acl.authorization.BasicPermissions.UPLOAD_STAGED_SSTABLE;
import static org.apache.cassandra.sidecar.acl.authorization.CassandraPermissions.SELECT;
import static org.apache.cassandra.sidecar.acl.authorization.FeaturePermission.BULK_READ_DIRECT;
import static org.apache.cassandra.sidecar.acl.authorization.FeaturePermission.BULK_WRITE_DIRECT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link FeaturePermission}
 */
class FeaturePermissionTest
{
    @Test
    void testFeaturePermissionAuthorizesAllChildPermissions()
    {
        Authorization bulkReadAuthorization = BULK_READ_DIRECT.permission().toAuthorization("data/university/student");

        assertThat(bulkReadAuthorization.verify(READ_RING_KEYSPACE_SCOPED.toAuthorization("data/university"))).isTrue();
        assertThat(bulkReadAuthorization.verify(READ_SCHEMA_KEYSPACE_SCOPED.toAuthorization("data/university"))).isTrue();
        assertThat(bulkReadAuthorization.verify(CREATE_SNAPSHOT.toAuthorization("data/university/student"))).isTrue();
        assertThat(bulkReadAuthorization.verify(READ_SNAPSHOT.toAuthorization("data/university/student"))).isTrue();
        assertThat(bulkReadAuthorization.verify(DELETE_SNAPSHOT.toAuthorization("data/university/student"))).isTrue();
        assertThat(bulkReadAuthorization.verify(STREAM_SNAPSHOT.toAuthorization("data/university/student"))).isTrue();
        assertThat(bulkReadAuthorization.verify(SELECT.toAuthorization("data/university/student"))).isTrue();

        assertThat(bulkReadAuthorization.verify(UPLOAD_STAGED_SSTABLE.toAuthorization("data/university/student"))).isFalse();
        assertThat(bulkReadAuthorization.verify(IMPORT_STAGED_SSTABLE.toAuthorization("data/university/student"))).isFalse();
        assertThat(bulkReadAuthorization.verify(DELETE_STAGED_SSTABLE.toAuthorization("data/university/student"))).isFalse();

        Authorization bulkWriteAuthorization = BULK_WRITE_DIRECT.permission().toAuthorization("data/university/student");

        assertThat(bulkWriteAuthorization.verify(READ_SCHEMA_KEYSPACE_SCOPED.toAuthorization("data/university"))).isTrue();
        assertThat(bulkWriteAuthorization.verify(READ_TOPOLOGY.toAuthorization("data/university"))).isTrue();
        assertThat(bulkWriteAuthorization.verify(UPLOAD_STAGED_SSTABLE.toAuthorization("data/university/student"))).isTrue();
        assertThat(bulkWriteAuthorization.verify(IMPORT_STAGED_SSTABLE.toAuthorization("data/university/student"))).isTrue();
        assertThat(bulkWriteAuthorization.verify(DELETE_STAGED_SSTABLE.toAuthorization("data/university/student"))).isTrue();

        assertThat(bulkWriteAuthorization.verify(READ_RING.toAuthorization("cluster"))).isFalse();
        assertThat(bulkWriteAuthorization.verify(READ_SNAPSHOT.toAuthorization("data/university/student"))).isFalse();
        assertThat(bulkWriteAuthorization.verify(STREAM_SNAPSHOT.toAuthorization("data/university/student"))).isFalse();
        assertThat(bulkWriteAuthorization.verify(SELECT.toAuthorization("data/university/student"))).isFalse();
    }

    @Test
    void testFeaturePermissionAcrossTables()
    {
        Authorization bulkReadAuthorization = BULK_READ_DIRECT.permission().toAuthorization("data/university/*");

        assertThat(bulkReadAuthorization.verify(READ_RING_KEYSPACE_SCOPED.toAuthorization("data/university"))).isTrue();
        assertThat(bulkReadAuthorization.verify(READ_SCHEMA_KEYSPACE_SCOPED.toAuthorization("data/university"))).isTrue();
        assertThat(bulkReadAuthorization.verify(CREATE_SNAPSHOT.toAuthorization("data/university/*"))).isTrue();
        assertThat(bulkReadAuthorization.verify(READ_SNAPSHOT.toAuthorization("data/university/*"))).isTrue();
        assertThat(bulkReadAuthorization.verify(DELETE_SNAPSHOT.toAuthorization("data/university/*"))).isTrue();
        assertThat(bulkReadAuthorization.verify(STREAM_SNAPSHOT.toAuthorization("data/university/*"))).isTrue();
        assertThat(bulkReadAuthorization.verify(SELECT.toAuthorization("data/university/*"))).isTrue();

        assertThat(bulkReadAuthorization.verify(UPLOAD_STAGED_SSTABLE.toAuthorization("data/university/*"))).isFalse();
        assertThat(bulkReadAuthorization.verify(IMPORT_STAGED_SSTABLE.toAuthorization("data/university/*"))).isFalse();
        assertThat(bulkReadAuthorization.verify(DELETE_STAGED_SSTABLE.toAuthorization("data/university/*"))).isFalse();

        Authorization bulkWriteAuthorization = BULK_WRITE_DIRECT.permission().toAuthorization("data/university/*");

        assertThat(bulkWriteAuthorization.verify(READ_SCHEMA.toAuthorization("data/university"))).isTrue();
        assertThat(bulkWriteAuthorization.verify(READ_TOPOLOGY.toAuthorization("data/university"))).isTrue();
        assertThat(bulkWriteAuthorization.verify(UPLOAD_STAGED_SSTABLE.toAuthorization("data/university/*"))).isTrue();
        assertThat(bulkWriteAuthorization.verify(IMPORT_STAGED_SSTABLE.toAuthorization("data/university/*"))).isTrue();
        assertThat(bulkWriteAuthorization.verify(DELETE_STAGED_SSTABLE.toAuthorization("data/university/*"))).isTrue();

        assertThat(bulkWriteAuthorization.verify(READ_RING.toAuthorization("data/university"))).isFalse();
        assertThat(bulkWriteAuthorization.verify(READ_SNAPSHOT.toAuthorization("data/university/*"))).isFalse();
        assertThat(bulkWriteAuthorization.verify(STREAM_SNAPSHOT.toAuthorization("data/university/*"))).isFalse();
        assertThat(bulkWriteAuthorization.verify(SELECT.toAuthorization("data/university/*"))).isFalse();
    }

    @Test
    void testNewFeatureWithNewResources()
    {
        ResourceScope dcScope = new ResourceScope()
        {
            public String variableAwareResource()
            {
                return "{dc}";
            }

            public String resolveWithResource(String resource)
            {
                return resource;
            }

            public Set<String> expandedResources()
            {
                return Collections.emptySet();
            }
        };
        Permission repairPermission = new StandardPermission("REPAIR").withScope(dcScope);

        CompositePermission compositePermission
        = new CompositePermission("featureX", Collections.singletonList(repairPermission));

        Authorization compositeAuthorization = compositePermission.toAuthorization("DC1");

        assertThat(compositeAuthorization.verify(repairPermission.toAuthorization("DC1"))).isTrue();
        assertThat(compositeAuthorization.verify(repairPermission.toAuthorization("DC2"))).isFalse();
    }

    @Test
    void testMatchedBasicPermissionsRetrieved()
    {
        Permission readPermission = FeaturePermission.fromName("ANALYTICS:READ_DIRECT");
        CompositePermission readCompositePermission = (CompositePermission) readPermission;
        assertThat(readCompositePermission).isNotNull();
        // has basic permissions for bulk read feature
        assertThat(readCompositePermission.childPermissions().size()).isEqualTo(7);

        Permission readWritePermission = FeaturePermission.fromName("ANALYTICS:READ_DIRECT,WRITE_DIRECT");
        CompositePermission readWriteCompositePermission = (CompositePermission) readWritePermission;
        assertThat(readWriteCompositePermission).isNotNull();
        // has basic permissions for both bulk read and bulk write feature
        assertThat(readWriteCompositePermission.childPermissions().size()).isEqualTo(14);
    }
}
