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
 * Permissions for features
 */
public class FeaturePermissions
{
    // Bulk Analytics permissions
    public static final Permission BULK_READ_DIRECT = new WildcardPermission("BULK_READ:DIRECT");
    public static final Permission BULK_WRITE_DIRECT = new WildcardPermission("BULK_WRITE:DIRECT");
    public static final Permission BULK_WRITE_S3_COMPAT = new WildcardPermission("BULK_WRITE:S3_COMPAT");

    // cdc related permissions
    public static final Permission CDC = new StandardPermission("CDC");
}
