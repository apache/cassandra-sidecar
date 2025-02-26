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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;

import static org.apache.cassandra.sidecar.common.utils.StringUtils.isNotEmpty;
import static org.apache.cassandra.sidecar.common.utils.StringUtils.isNullOrEmpty;

/**
 * {@link JwtParameterExtractor} parses necessary JWT configuration from parameters passed during authenticator setup.
 */
public class JwtParameterExtractor
{
    protected static final String SITE_SUFFIX = "/.well-known/openid-configuration";

    private static final Logger LOGGER = LoggerFactory.getLogger(JwtParameterExtractor.class);
    private static final String SITE_PARAM_KEY = "site";
    private static final String CLIENT_ID_PARAM_KEY = "client_id";
    private static final String SCOPE_SEPARATOR_PARAM_KEY = "scope_separator";
    private static final String DEFAULT_SCOPE_SEPARATOR = ",";
    private static final String SCOPES_SUPPORTED_PARAM_KEY = "scopes_supported";
    private static final String CONFIG_DISCOVER_INTERVAL_PARAM_KEY = "config_discover_interval";
    private static final SecondBoundConfiguration DEFAULT_CONFIG_DISCOVER_INTERVAL
    = SecondBoundConfiguration.parse("1m");

    private final String site;
    private final String clientId;
    private final SecondBoundConfiguration configDiscoverInterval;
    private final List<String> scopes;

    public JwtParameterExtractor(Map<String, String> parameters)
    {
        validate(parameters);
        this.site = removeSiteSuffix(parameters);
        this.clientId = parameters.get(CLIENT_ID_PARAM_KEY);
        this.scopes = buildScopes(parameters);
        this.configDiscoverInterval = parameters.containsKey(CONFIG_DISCOVER_INTERVAL_PARAM_KEY)
                                      ? SecondBoundConfiguration.parse(parameters.get(CONFIG_DISCOVER_INTERVAL_PARAM_KEY))
                                      : DEFAULT_CONFIG_DISCOVER_INTERVAL;
    }

    /**
     * @return site to dynamically retrieve the configuration information of an OpenID provider
     */
    public String site()
    {
        return site;
    }

    /**
     * @return clientId is a unique identifier used to identity applications/users
     */
    public String clientId()
    {
        return clientId;
    }

    /**
     * @return scopes granted to a user
     */
    public List<String> scopes()
    {
        return scopes;
    }

    /**
     * @return interval at which {@link io.vertx.ext.auth.oauth2.providers.OpenIDConnectAuth} discover is called to
     * dynamically retrieve configuration information of an OpenID provider.
     */
    public SecondBoundConfiguration configDiscoverInterval()
    {
        return configDiscoverInterval;
    }

    private void validate(Map<String, String> parameters)
    {
        if (parameters == null)
        {
            throw new IllegalArgumentException("JWT parameters can not be null");
        }

        validateParameterPresence(parameters, SITE_PARAM_KEY);
        validateParameterPresence(parameters, CLIENT_ID_PARAM_KEY);
    }

    private void validateParameterPresence(Map<String, String> parameters, String paramKey)
    {
        if (!parameters.containsKey(paramKey) || isNullOrEmpty(parameters.get(paramKey)))
        {
            throw new IllegalArgumentException(String.format("Missing %s JWT parameter", paramKey));
        }
    }

    private String removeSiteSuffix(Map<String, String> parameters)
    {
        String site = parameters.get(SITE_PARAM_KEY);
        if (site.endsWith(SITE_SUFFIX))
        {
            LOGGER.info("Removing site suffix {}, it is added during OpenID discover", SITE_SUFFIX);
            return site.substring(0, site.length() - SITE_SUFFIX.length());
        }
        return site;
    }

    private List<String> buildScopes(Map<String, String> parameters)
    {
        List<String> scopes = new ArrayList<>();
        if (isNotEmpty(parameters.get(SCOPES_SUPPORTED_PARAM_KEY)))
        {
            String delimiter = isNotEmpty(parameters.get(SCOPE_SEPARATOR_PARAM_KEY))
                               ? parameters.get(SCOPE_SEPARATOR_PARAM_KEY)
                               : DEFAULT_SCOPE_SEPARATOR;
            scopes.addAll(Arrays.asList(parameters.get(SCOPES_SUPPORTED_PARAM_KEY).split(delimiter)));
        }
        return scopes;
    }
}
