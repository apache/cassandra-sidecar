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

package org.apache.cassandra.sidecar.acl.authentication;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.Future;
import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.CredentialValidationException;
import io.vertx.ext.auth.mtls.impl.SpiffeIdentityExtractor;
import org.apache.cassandra.sidecar.acl.AdminIdentityResolver;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;

/**
 * {@link CassandraIdentityExtractor} verifies {@code SPIFFE} identities extracted from certificate are mapped
 * to a valid role in Cassandra or a pre-configured administrative identity.
 */
public class CassandraIdentityExtractor extends SpiffeIdentityExtractor
{
    private final IdentityToRoleCache identityToRoleCache;
    private final AdminIdentityResolver adminIdentityResolver;

    public CassandraIdentityExtractor(AdminIdentityResolver adminIdentityResolver,
                                      IdentityToRoleCache identityToRoleCache)
    {
        this.identityToRoleCache = identityToRoleCache;
        this.adminIdentityResolver = adminIdentityResolver;
    }

    @Override
    public Future<List<String>> validIdentities(CertificateCredentials certificateCredentials)
    {
        return super.validIdentities(certificateCredentials)
                    .compose(identities -> {
                        List<Future<Boolean>> validityCheckFutures = new ArrayList<>();
                        for (String identity : identities)
                        {
                            // Sidecar recognizes admin identities or identities in identity_to_role table as authenticated
                            Future<Boolean> isValidFuture
                            = adminIdentityResolver.isAdmin(identity)
                                                   .compose(isAdmin -> isAdmin ? Future.succeededFuture(true)
                                                                               : identityToRoleCache.containsKey(identity))
                                                   .otherwise(false);
                            validityCheckFutures.add(isValidFuture);
                        }
                        return Future.all(validityCheckFutures)
                                     .map(compositeFuture -> {
                                         List<String> allowedIdentities = new ArrayList<>(identities.size());
                                         for (int i = 0; i < identities.size(); i++)
                                         {
                                             if (compositeFuture.<Boolean>resultAt(i))
                                             {
                                                 allowedIdentities.add(identities.get(i));
                                             }
                                         }
                                         if (allowedIdentities.isEmpty())
                                         {
                                             throw new CredentialValidationException("Could not extract valid" +
                                                                                     " identities from certificate");
                                         }
                                         return allowedIdentities;
                                     });
                    });
    }
}
