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

package org.apache.cassandra.sidecar.modules;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.web.handler.ChainAuthHandler;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;
import org.apache.cassandra.sidecar.acl.authentication.AuthenticationHandlerFactory;
import org.apache.cassandra.sidecar.acl.authentication.AuthenticationHandlerFactoryRegistry;
import org.apache.cassandra.sidecar.acl.authentication.JwtAuthenticationHandlerFactory;
import org.apache.cassandra.sidecar.acl.authentication.JwtRoleProcessor;
import org.apache.cassandra.sidecar.acl.authentication.JwtRoleProcessorImpl;
import org.apache.cassandra.sidecar.acl.authentication.MutualTlsAuthenticationHandlerFactory;
import org.apache.cassandra.sidecar.acl.authorization.AdminIdentityResolver;
import org.apache.cassandra.sidecar.acl.authorization.AllowAllAuthorizationProvider;
import org.apache.cassandra.sidecar.acl.authorization.AuthorizationParameterValidateHandler;
import org.apache.cassandra.sidecar.acl.authorization.PermissionFactory;
import org.apache.cassandra.sidecar.acl.authorization.PermissionFactoryImpl;
import org.apache.cassandra.sidecar.acl.authorization.RoleAuthorizationsCache;
import org.apache.cassandra.sidecar.acl.authorization.RoleBasedAuthorizationProvider;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.schema.SidecarRolePermissionsSchema;
import org.apache.cassandra.sidecar.db.schema.SystemAuthSchema;
import org.apache.cassandra.sidecar.db.schema.TableSchema;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.TableSchemaMapKeys;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.RoutingOrder;
import org.apache.cassandra.sidecar.routes.VertxRoute;

/**
 * Provides authentication and authorization (role-based) capability
 */
public class AuthModule extends AbstractModule
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthModule.class);
    
    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.SystemAuthSchemaKey.class)
    TableSchema systemAuthSchema() // Note that it returns TableSchema, not CassandraSystemTableSchema, in order to locate the same mapBinder
    {
        return new SystemAuthSchema();
    }
    
    @ProvidesIntoMap
    @KeyClassMapKey(TableSchemaMapKeys.SidecarRolePermissionsSchemaKey.class)
    TableSchema sidecarRolePermissionSchema(SidecarConfiguration sidecarConfiguration)
    {
        return new SidecarRolePermissionsSchema(sidecarConfiguration);
    }

    @Provides
    @Singleton
    RouteBuilder.Factory accessProtectedRouteBuilderFactory(SidecarConfiguration sidecarConfiguration,
                                                            AuthorizationProvider authorizationProvider,
                                                            AdminIdentityResolver adminIdentityResolver,
                                                            AuthorizationParameterValidateHandler authorizationParameterValidateHandler)
    {
        return new RouteBuilder.Factory(sidecarConfiguration.accessControlConfiguration(),
                                        authorizationProvider,
                                        adminIdentityResolver,
                                        authorizationParameterValidateHandler);
    }

    @Provides
    @Singleton
    public JwtRoleProcessor roleProcessor(IdentityToRoleCache identityToRoleCache)
    {
        return new JwtRoleProcessorImpl(identityToRoleCache);
    }

    @Provides
    @Singleton
    public AuthenticationHandlerFactoryRegistry authNHandlerFactoryRegistry(MutualTlsAuthenticationHandlerFactory mTLSAuthHandlerFactory,
                                                                            JwtAuthenticationHandlerFactory jwtAuthHandlerFactory)
    {
        AuthenticationHandlerFactoryRegistry registry = new AuthenticationHandlerFactoryRegistry();
        registry.register(mTLSAuthHandlerFactory);
        registry.register(jwtAuthHandlerFactory);
        return registry;
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.GlobalChainAuthHandlerKey.class)
    VertxRoute chainAuthHandler(Vertx vertx,
                                SidecarConfiguration sidecarConfiguration,
                                AuthenticationHandlerFactoryRegistry registry) throws ConfigurationException
    {
        AccessControlConfiguration accessControlConfiguration = sidecarConfiguration.accessControlConfiguration();
        if (!accessControlConfiguration.enabled())
        {
            // do nothing
            return VertxRoute.EMPTY;
        }

        // authenticator is enabled
        List<ParameterizedClassConfiguration> authList = accessControlConfiguration.authenticatorsConfiguration();
        if (authList == null || authList.isEmpty())
        {
            LOGGER.error("Access control was enabled, but there are no configured authenticators");
            throw new ConfigurationException("Invalid access control configuration. There are no configured authenticators");
        }

        ChainAuthHandler chainAuthHandler = ChainAuthHandler.any();
        for (ParameterizedClassConfiguration config : authList)
        {
            AuthenticationHandlerFactory factory = registry.getFactory(config.className());

            if (factory == null)
            {
                throw new RuntimeException(String.format("Implementation for class %s has not been registered",
                                                         config.className()));
            }
            chainAuthHandler.add(factory.create(vertx, accessControlConfiguration, config.namedParameters()));
        }

        return VertxRoute.create(router -> router.route()
                                                 // eval after the highest ordered handlers, e.g. logger and timeout handlers
                                                 .order(RoutingOrder.HIGHER.order)
                                                 .handler(chainAuthHandler));
    }

    @Provides
    @Singleton
    PermissionFactory permissionFactory()
    {
        return new PermissionFactoryImpl();
    }

    @Provides
    @Singleton
    AuthorizationProvider authorizationProvider(SidecarConfiguration sidecarConfiguration,
                                                RoleAuthorizationsCache roleAuthorizationsCache)
    {
        AccessControlConfiguration accessControlConfiguration = sidecarConfiguration.accessControlConfiguration();
        if (!accessControlConfiguration.enabled())
        {
            return new AllowAllAuthorizationProvider();
        }

        ParameterizedClassConfiguration config = accessControlConfiguration.authorizerConfiguration();
        if (config == null)
        {
            throw new ConfigurationException("Access control is enabled, but authorizer not set");
        }

        if (config.className().equalsIgnoreCase(AllowAllAuthorizationProvider.class.getName()))
        {
            return new AllowAllAuthorizationProvider();
        }
        if (config.className().equalsIgnoreCase(RoleBasedAuthorizationProvider.class.getName()))
        {
            return new RoleBasedAuthorizationProvider(roleAuthorizationsCache);
        }
        throw new ConfigurationException("Unrecognized authorization provider " + config.className() + " set");
    }
}
