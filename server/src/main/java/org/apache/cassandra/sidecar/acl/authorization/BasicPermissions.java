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

/**
 * Basic permissions are the building blocks of the permission-ing system in Sidecar. It offers more granular
 * permissions on specific targets which are listed here. The majority of Sidecar permissions are represented in
 * format {@code domain}:{@code action}.
 * <p>
 * For example, the SNAPSHOT:CREATE permission, CREATE action is allowed for the SNAPSHOT domain. Sample actions are
 * CREATE, READ, EDIT, DELETE, IMPORT, UPLOAD, START etc.
 * <p>
 * Domain aware permissions are supported with ':' wildcard parts divider. Wildcard token '*' is restricted
 * to avoid unpredictable behavior.
 * <p>
 * Some examples of domain aware permissions are:
 * - SNAPSHOT:CREATE,READ,DELETE allows SNAPSHOT:CREATE, SNAPSHOT:READ and SNAPSHOT:DELETE.
 */
public class BasicPermissions
{
    // SSTable staging related permissions
    public static final Permission UPLOAD_STAGED_SSTABLE = new DomainAwarePermission("STAGED_SSTABLE:UPLOAD");
    public static final Permission IMPORT_STAGED_SSTABLE = new DomainAwarePermission("STAGED_SSTABLE:IMPORT");
    public static final Permission DELETE_STAGED_SSTABLE = new DomainAwarePermission("STAGED_SSTABLE:DELETE");

    // snapshot related permissions
    public static final Permission CREATE_SNAPSHOT = new DomainAwarePermission("SNAPSHOT:CREATE");
    public static final Permission READ_SNAPSHOT = new DomainAwarePermission("SNAPSHOT:READ");
    public static final Permission DELETE_SNAPSHOT = new DomainAwarePermission("SNAPSHOT:DELETE");
    public static final Permission STREAM_SNAPSHOT = new DomainAwarePermission("SNAPSHOT:STREAM");

    // restore job related permissions
    public static final Permission CREATE_RESTORE_JOB = new DomainAwarePermission("RESTORE_JOB:CREATE");
    public static final Permission READ_RESTORE_JOB = new DomainAwarePermission("RESTORE_JOB:READ");
    public static final Permission EDIT_RESTORE_JOB = new DomainAwarePermission("RESTORE_JOB:EDIT");
    public static final Permission DELETE_RESTORE_JOB = new DomainAwarePermission("RESTORE_JOB:DELETE");

    // cdc related permissions
    public static final Permission CDC = new StandardPermission("CDC");

    // sidecar operation related permissions
    public static final Permission READ_OPERATIONAL_JOB = new DomainAwarePermission("OPERATIONAL_JOB:READ");
    public static final Permission DECOMMISSION_NODE = new DomainAwarePermission("NODE:DECOMMISSION");

    // cassandra cluster related permissions
    public static final Permission READ_SCHEMA = new DomainAwarePermission("SCHEMA:READ");
    public static final Permission READ_GOSSIP = new DomainAwarePermission("GOSSIP:READ");
    public static final Permission READ_RING = new DomainAwarePermission("RING:READ");
    public static final Permission READ_TOPOLOGY = new DomainAwarePermission("TOPOLOGY:READ");

    // cassandra stats permissions
    public static final Permission STATS = new StandardPermission("STATS");
}
