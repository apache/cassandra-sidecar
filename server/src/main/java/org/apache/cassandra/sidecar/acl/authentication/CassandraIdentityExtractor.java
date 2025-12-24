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
                            Future<Boolean> isAdminFuture = adminIdentityResolver.isAdmin(identity);
                            // Sidecar recognizes identities in identity_to_role table as authenticated
                            Future<Boolean> inCacheFuture = identityToRoleCache.containsKey(identity);

                            Future<Boolean> isValidFuture
                            = Future.all(isAdminFuture, inCacheFuture)
                                    .map(compositeFuture -> {
                                        boolean isAdmin = compositeFuture.resultAt(0);
                                        boolean inCache = compositeFuture.resultAt(1);
                                        return isAdmin || inCache;
                                    });
                            validityCheckFutures.add(isValidFuture);
                        }
                        return Future.all(validityCheckFutures)
                                     .map(compositeFuture -> {
                                         List<Boolean> validityResults = compositeFuture.list();
                                         List<String> allowedIdentities = new ArrayList<>();
                                         for (int i = 0; i < identities.size(); i++)
                                         {
                                             if (validityResults.get(i))
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
