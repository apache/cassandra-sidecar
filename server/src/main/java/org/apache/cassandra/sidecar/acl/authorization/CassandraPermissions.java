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
 * Cassandra permissions allowed. These map to Cassandra permissions in
 * <a href="https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/auth/Permission.java">
 *     org.apache.cassandra.auth.Permission</a>
 * under the Cassandra codebase.
 * <p>
 * Note: CassandraPermissions by default have no scope set in them, since these permissions can be used to create
 * Authorization across resource scopes.
 */
public class CassandraPermissions
{
    public static final Permission CREATE = new StandardPermission("CREATE");
    public static final Permission ALTER = new StandardPermission("ALTER");
    public static final Permission DROP = new StandardPermission("DROP");
    public static final Permission SELECT = new StandardPermission("SELECT");
    public static final Permission MODIFY = new StandardPermission("MODIFY");
    public static final Permission AUTHORIZE = new StandardPermission("AUTHORIZE");
    public static final Permission DESCRIBE = new StandardPermission("DESCRIBE");
    public static final Permission EXECUTE = new StandardPermission("EXECUTE");
    public static final Permission UNMASK = new StandardPermission("UNMASK");
    public static final Permission SELECT_MASKED = new StandardPermission("SELECT_MASKED");
}
